package servers

import (
	etcdclient "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	DEFAULT_ETCD        = "http://10.211.55.84:2379"
	DEFAULT_SERVER_PATH = "backends"
	DEFAULT_NAME_FILE   = "backends/names"
)

//单个客户端连接
type client struct {
	key  string //etcd key
	conn *grpc.ClientConn
}

type server struct {
	clients []client
	idx     uint32 //for round-robin purpos
}

type server_pool struct {
	servers           map[string]*server
	known_names       map[string]bool
	enable_name_check bool
	client            etcdclient.Client
	callbacks         map[string][]chan string
	sync.RWMutex
}

var (
	_default_pool server_pool
	once          sync.Once
)

func Init(names ...string) {
	once.Do(func() { _default_pool.init(names...) })
}

func (p *server_pool) init(names ...string) {
	//etcd client
	machines := []string{DEFAULT_ETCD}
	if env := os.Getenv("ETCD_HOST"); env != "" {
		machines = strings.Split(env, ";")
	}
	//init etcd client
	cfg := etcdclient.Config{
		Endpoints: machines,
		Transport: etcdclient.DefaultTransport,
	}
	c, err := etcdclient.New(cfg)
	if err != nil {
		log.Panic(err)
		os.Exit(-1)
	}
	p.client = c

	//init
	p.servers = make(map[string]*server)
	p.known_names = make(map[string]bool)

	//names init
	if len(names) == 0 {
		names = p.load_names() //try read from names.txt
	}
	if len(names) > 0 {
		p.enable_name_check = true
	}

	log.Println("all servers names:", names)
	for _, v := range names {
		p.known_names[DEFAULT_SERVER_PATH+"/"+strings.TrimSpace(v)] = true
	}
	//start connection
	p.connect_all(DEFAULT_SERVER_PATH)
}

//get stored server name
func (p *server_pool) load_names() []string {
	kAPI := etcdclient.NewKeysAPI(p.client)
	//get the keys under directory
	log.Println("reading names:", DEFAULT_NAME_FILE)
	resp, err := kAPI.Get(context.Background(), DEFAULT_NAME_FILE, nil)
	if err != nil {
		log.Println(err)
		return nil
	}
	//validation check
	if resp.Node.Dir {
		log.Println("names is not a file")
		return nil
	}
	return strings.Split(resp.Node.Value, "\n")
}

//connect to all servers
func (p *server_pool) connect_all(directory string) {
	kAPI := etcdclient.NewKeysAPI(p.client)
	//get the keys under directory
	log.Println("connecting servers under:", directory)
	resp, err := kAPI.Get(context.Background(), directory, nil)
	if err != nil {
		log.Println(err)
	}
	//validation check
	if !resp.Node.Dir {
		log.Println("no a directory")
		return
	}

	for _, node := range resp.Node.Nodes {
		if node.Dir {
			for _, server := range node.Nodes {
				p.add_server(server.Key, server.Value)
			}
		}

	}
	log.Println("servers add complete")
	go p.watcher()

}

//watcher for data change in etcd directory
func (p *server_pool) watcher() {
	kAPI := etcdclient.NewKeysAPI(p.client)
	w := kAPI.Watcher(DEFAULT_SERVER_PATH, &etcdclient.WatcherOptions{Recursive: true})
	for {
		resp, err := w.Next(context.Background())

		if err != nil {
			continue
		}

		if resp.Node.Dir {
			continue
		}

		switch resp.Action {
		case "set", "create", "update", "compareAndSwap":
			p.add_server(resp.Node.Key, resp.Node.Value)
		case "delete":
			p.remove_server(resp.PrevNode.Key)

		}
	}

}

//add a server
func (p *server_pool) add_server(key, value string) {
	p.Lock()
	defer p.Unlock()
	//name check
	server_name := filepath.Dir(key)
	if p.enable_name_check && !p.known_names[server_name] {
		return
	}

	//try new server kind init
	if p.servers[server_name] == nil {
		p.servers[server_name] = &server{}
	}

	//create server connection
	server := p.servers[server_name]
	if conn, err := grpc.Dial(value, grpc.WithBlock(), grpc.WithInsecure()); err == nil {
		server.clients = append(server.clients, client{key, conn})
		log.Println("server added:", key, "-->", value)
		for k := range p.callbacks[server_name] {
			select {
			case p.callbacks[server_name][k] <- key:
			default:
			}
		}
	} else {
		log.Println("did not connect:", key, "-->", value, "error:", err)
	}
}

// remove a server
func (p *server_pool) remove_server(key string) {
	p.Lock()
	defer p.Unlock()
	// name check
	server_name := filepath.Dir(key)
	if p.enable_name_check && !p.known_names[server_name] {
		return
	}

	// check server kind
	server := p.servers[server_name]
	if server == nil {
		log.Println("no such server:", server_name)
		return
	}

	// remove a server
	for k := range server.clients {
		if server.clients[k].key == key { // deletion
			server.clients[k].conn.Close()
			server.clients = append(server.clients[:k], server.clients[k+1:]...)
			log.Println("server removed:", key)
			return
		}
	}
}

// provide a specific key for a server, eg:
// path:/backends/snowflake, id:s1
//
// the full cannonical path for this server is:
//			/backends/snowflake/s1
func (p *server_pool) get_server_with_id(path string, id string) *grpc.ClientConn {
	p.RLock()
	defer p.RUnlock()
	// check existence
	server := p.servers[path]
	if server == nil {
		return nil
	}
	if len(server.clients) == 0 {
		return nil
	}

	// loop find a server with id
	fullpath := string(path) + "/" + id
	for k := range server.clients {
		if server.clients[k].key == fullpath {
			return server.clients[k].conn
		}
	}

	return nil
}

// get a server in round-robin style
// especially useful for load-balance with state-less servers
func (p *server_pool) get_server(path string) (conn *grpc.ClientConn, key string) {
	p.RLock()
	defer p.RUnlock()
	// check existence
	server := p.servers[path]
	if server == nil {
		return nil, ""
	}

	if len(server.clients) == 0 {
		return nil, ""
	}

	// get a server in round-robind style,
	idx := int(atomic.AddUint32(&server.idx, 1)) % len(server.clients)
	return server.clients[idx].conn, server.clients[idx].key
}

func (p *server_pool) register_callback(path string, callback chan string) {
	p.Lock()
	defer p.Unlock()
	if p.callbacks == nil {
		p.callbacks = make(map[string][]chan string)
	}

	p.callbacks[path] = append(p.callbacks[path], callback)
	if s, ok := p.servers[path]; ok {
		for k := range s.clients {
			callback <- s.clients[k].key
		}
	}
	log.Println("register callback on:", path)
}

/////////////////////////////////////////////////////////////////
// Wrappers
func GetService(path string) *grpc.ClientConn {
	conn, _ := _default_pool.get_server(path)
	return conn
}

func GetService2(path string) (*grpc.ClientConn, string) {
	conn, key := _default_pool.get_server(path)
	return conn, key
}

func GetServiceWithId(path string, id string) *grpc.ClientConn {
	return _default_pool.get_server_with_id(path, id)
}

func RegisterCallback(path string, callback chan string) {
	_default_pool.register_callback(path, callback)
}
