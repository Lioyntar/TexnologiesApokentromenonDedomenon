import hashlib
import json
import os
import socket
import threading
import time

try:
    from bplustree import BPlusTree
    HAS_BPLUSTREE = True
except ImportError:
    HAS_BPLUSTREE = False

def safe_remove_db(filepath):
    try:
        if os.path.exists(filepath): os.remove(filepath)
        if os.path.exists(filepath + "-wal"): os.remove(filepath + "-wal")
        return True
    except: return False

class PastryNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id_int = self._generate_hash(f"{ip}:{port}")
        self.id_hex = format(self.id_int, '040x') 
        
        folder_name = "node_storage"
        storage_dir = os.path.join(os.getcwd(), folder_name)
        os.makedirs(storage_dir, exist_ok=True)
        
        filename = f"storage_pastry_{self.id_hex[:10]}.db"
        self.db_filename = os.path.join(storage_dir, filename)
        
        if HAS_BPLUSTREE:
            if not safe_remove_db(self.db_filename):
                self.db_filename = os.path.join(storage_dir, f"storage_pastry_{self.id_hex[:10]}_{int(time.time())}.db")
            self.storage = BPlusTree(self.db_filename, order=50, key_size=32)
        else:
            self.storage = {} 
            
        # State: List of Dicts {'id_hex': ..., 'ip': ..., 'port': ...}
        self.leaf_set = []      
        self.node_info = {'id_hex': self.id_hex, 'ip': self.ip, 'port': self.port}

        # Networking
        self.running = True
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.daemon = True
        self.server_thread.start()

    def _generate_hash(self, key):
        sha1 = hashlib.sha1(key.encode('utf-8'))
        return int(sha1.hexdigest(), 16)

    # --- NETWORKING ---
    def send_request(self, target_node, command, payload={}):
        if target_node['id_hex'] == self.id_hex:
            return self.handle_local_command(command, payload)
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((target_node['ip'], target_node['port']))
                msg = {'command': command, 'payload': payload}
                s.sendall(json.dumps(msg).encode('utf-8'))
                response = s.recv(4096 * 4)
                return json.loads(response.decode('utf-8'))
        except:
            return None

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(5)
            while self.running:
                try:
                    conn, addr = s.accept()
                    threading.Thread(target=self.handle_client, args=(conn,)).start()
                except: break

    def handle_client(self, conn):
        with conn:
            try:
                data = conn.recv(4096 * 4)
                if not data: return
                msg = json.loads(data.decode('utf-8'))
                response = self.handle_local_command(msg['command'], msg['payload'])
                conn.sendall(json.dumps(response).encode('utf-8'))
            except: 
                conn.sendall(json.dumps({'status': 'error'}).encode('utf-8'))

    def handle_local_command(self, command, payload):
        if command == 'route':
            return self.route(payload['key_hex'])
        elif command == 'lookup_recursive':
            return self.lookup_recursive(payload['key_hex'], payload['hops'])
        elif command == 'insert_local':
            return self.insert_local(payload['key_int'], payload['data'])
        elif command == 'lookup_local':
            return self.lookup_local(payload['key_int'])
        elif command == 'delete_local':
            return self.delete_local(payload['key_int'])
        elif command == 'get_leaf_set':
            return {'leaf_set': self.leaf_set}
        elif command == 'update_leaf_set':
            self.leaf_set = payload['leaf_set']
            return {'status': 'ok'}
        return {'error': 'unknown'}

    # --- LOGIC ---
    def route(self, key_hex):
        # Simply find the node in leaf_set numerically closest to key
        best_node = self.node_info
        best_dist = abs(int(self.id_hex, 16) - int(key_hex, 16))
        
        for node in self.leaf_set:
            dist = abs(int(node['id_hex'], 16) - int(key_hex, 16))
            if dist < best_dist:
                best_dist = dist
                best_node = node
        
        # If I am the best, return me
        if best_node['id_hex'] == self.id_hex:
            return {'node': self.node_info, 'forward': False}
        else:
            return {'node': best_node, 'forward': True}

    def lookup_recursive(self, key_hex, hops):
        res = self.route(key_hex)
        target = res['node']
        
        if not res['forward']:
            return {'node': self.node_info, 'hops': hops}
        
        # Forward request
        try:
            return self.send_request(target, 'lookup_recursive', {'key_hex': key_hex, 'hops': hops + 1})
        except:
            return {'node': self.node_info, 'hops': hops} # Fallback

    def insert_key(self, title, data):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        
        # Find responsible
        res = self.lookup_recursive(key_hex, 0)
        target = res['node']
        self.send_request(target, 'insert_local', {'key_int': key_int, 'data': data})

    def insert_local(self, key_int, data):
        if HAS_BPLUSTREE:
            self.storage[key_int] = json.dumps(data).encode('utf-8')
        else:
            self.storage[key_int] = data
        return {'status': 'ok'}

    def update_key(self, title, new_data):
        self.insert_key(title, new_data)

    def delete_key(self, title):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        res = self.lookup_recursive(key_hex, 0)
        self.send_request(res['node'], 'delete_local', {'key_int': key_int})

    def delete_local(self, key_int):
        try:
            if HAS_BPLUSTREE:
                del self.storage[key_int]
            else:
                del self.storage[key_int]
        except: pass
        return {'status': 'ok'}

    def lookup_key(self, title):
        key_int = self._generate_hash(title)
        key_hex = format(key_int, '040x')
        
        res = self.lookup_recursive(key_hex, 0)
        target = res['node']
        hops = res['hops']
        
        data_res = self.send_request(target, 'lookup_local', {'key_int': key_int})
        return data_res['val'], hops

    def lookup_local(self, key_int):
        val = None
        if HAS_BPLUSTREE:
            try:
                b = self.storage.get(key_int)
                if b: val = json.loads(b.decode('utf-8'))
            except: pass
        else:
            val = self.storage.get(key_int)
        return {'val': val}

    def join(self, known_node):
        # REALISTIC JOIN: Get leaf set from a known node
        res = known_node.send_request(known_node.node_info, 'get_leaf_set', {})
        if res:
            # Combine known node + its leaves
            candidates = res['leaf_set'] + [known_node.node_info]
            # My leaf set is the 4 closest nodes from candidates (Simulating Partial View)
            candidates.sort(key=lambda x: abs(int(x['id_hex'], 16) - self.id_int))
            self.leaf_set = candidates[:4] # Only keep 4 neighbors
            
            # Inform neighbors about me
            for n in self.leaf_set:
                self.send_request(n, 'update_leaf_set', {'leaf_set': self.leaf_set}) # Simplification

    def leave(self):
        # Transfer keys to first neighbor
        if self.leaf_set:
            neighbor = self.leaf_set[0]
            if HAS_BPLUSTREE:
                try:
                    for k, v in self.storage.items():
                         data = json.loads(v.decode('utf-8'))
                         self.send_request(neighbor, 'insert_local', {'key_int': k, 'data': data})
                except: pass
        self.cleanup()

    def cleanup(self):
        self.running = False
        if HAS_BPLUSTREE:
            try: self.storage.close()
            except: pass