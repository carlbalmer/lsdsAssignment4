require"splay.base"
rpc = require"splay.urpc"
crypto = require"crypto"

-- addition to allow local run
if not job then
	-- may be used outside SPLAY deployments
	local utils = require("splay.utils")
	if #arg < 2 then  
		log:print("lua "..arg[0].." my_position nb_nodes")  
		os.exit()  
	else
		local pos, total = tonumber(arg[1]), tonumber(arg[2])  
		job = utils.generate_job(pos, total, 20001)  
	end
end

rpc.server(job.me.port)

-- addition for debugging

debug = true
debug_level = {
	main=false,
	tman_init=false,
	active_tman=false,
	passive_tman=false,
	selectPeer=false,
	rank_view=false,
	extractMessage=false,
	merge=false,
	bootstrap_chord=true,
	extract_view=false,
	printClosestAverage=false
	}

function logD(message,level)
	if debug and debug_level[level] then
		log:print(level.."()	"..message)
	end
end

--[[
******************************************************************************
                           PEER SAMPLING PARAMETERS
******************************************************************************
]]

c = 10
exch = 5
S = 3
H = 2
SEL = "rand"
pss_active_thread_period = 20 -- period in seconds
pss_debug = false

--[[
******************************************************************************
                     PEER SAMPLING SERVICE: DO NOT MODIFY
******************************************************************************
]]

-- variables: peer sampling
view = {}

-- utilities
function print_table(t)
	log:print("[ (size "..#t..")")
	for i=1,#t do
		log:print("  "..i.." : ".."["..t[i].peer.ip..":"..t[i].peer.port.."] - age: "..t[i].age.." - id: "..t[i].id)
	end
	log:print("]")
end

function set_of_peers_to_string(v)
	ret = ""; for i=1,#v do	ret = ret..v[i].id.." , age="..v[i].age.."; " end
	return ret
end

function print_set_of_peers(v,message)	
	if message then log:print(message) end
	log:print(set_of_peers_to_string(v))
end

function print_view(message)
	if message then log:print(message) end
	log:print("content of the view --> "..job.position..": "..set_of_peers_to_string(view))
end

-- peer sampling functions

function pss_selectPartner()
	if SEL == "rand" then return math.random(#view) end
	if SEL == "tail" then
		local ret_ind = -1 ; local ret_age = -1
		for i,p in pairs(view) do
			if (p.age > ret_age) then ret_ind = i end
		end
		assert (not (ret_ind == -1))
		return ret_ind
	end
end

function same_peer_but_different_ages(a,b)
	return a.peer.ip == b.peer.ip and a.peer.port == b.peer.port
end
function same_peer(a,b)
	return same_peer_but_different_ages(a,b) and a.age == b.age
end

function pss_selectToSend()
	-- create a new return buffer
	local toSend = {}
	-- append the local node view age 0
	table.insert(toSend,{peer={ip=job.me.ip,port=job.me.port},age=0,id=job.position})
	-- shuffle view
	view = misc.shuffle(view)
	-- move oldest H items to the end of the view
	--- 1. copy the view
	local tmp_view = misc.dup(view)
	--- 2. sort the items based on the age
	table.sort(tmp_view,function(a,b) return a.age < b.age end)
	--- 3. get the H largest aged elements from the tmp_view, remove them from the view 
	---    (we assume there are no duplicates in the view at this point!)
	---    and put them at the end of the view
	for i=(#tmp_view-H+1),#tmp_view do
		local ind = -1
		for j=1,#view do
			if same_peer(tmp_view[i],view[j]) then ind=j; break end
		end
		assert (not (ind == -1))
		elem = table.remove(view,ind)
		view[#view+1] = elem
	end

	-- append the first exch-1 elements of view to toSend
	for i=1,(exch-1) do
		toSend[#toSend+1]=view[i]
	end		

	return toSend
end

function pss_selectToKeep(received)
	if pss_debug then
		log:print("select to keep, node "..job.position)
		print_set_of_peers(received, "content of the received for "..job.position..": ")
		print_view()
	end
	-- concatenate the view and the received set of view items
	for j=1,#received do view[#view+1] = received[j] end
	
	-- remove duplicates from view
	-- note that we can't rely on sorting the table as we need its order later
	local i = 1	
	while i < #view-1 do
		for j=i+1,#view do
			if same_peer_but_different_ages(view[i],view[j]) then
				-- delete the oldest
				if view[i].age < view[j].age then 
					table.remove(view,j)
				else
					table.remove(view,i)
				end
				i = i - 1 -- we need to retest for i in case there is one more duplicate
				break
			end
		end
		i = i + 1
	end

	-- remove the min(H,#view-c) oldest items from view
	local o = math.min(H,#view-c)
	while o > 0 do
		-- brute force -- remove the oldest
		local oldest_index = -1
		local oldest_age = -1
		for i=1,#view do 
			if oldest_age < view[i].age then
				oldest_age = view[i].age
				oldest_index = i
			end
		end
		assert (not (oldest_index == -1))
		table.remove(view,oldest_index)
		o = o - 1
	end
	
	-- remove the min(S,#view-c) head items from view
	o = math.min(S,#view-c)
	while o > 0 do
		table.remove(view,1) -- not optimal
		o = o - 1
	end
	
	-- in the case there still are too many peers in the view, remove at random
	while #view > c do table.remove(view,math.random(#view)) end

	assert (#view <= c)
end 

no_passive_while_active_lock = events.lock()

function pss_passiveThread(from,buffer)
	no_passive_while_active_lock:lock()
	if pss_debug then
		print_view("passiveThread ("..job.position.."): entering")
		print_set_of_peers(buffer,"passiveThread ("..job.position.."): received from "..from)
	end
	local ret = pss_selectToSend()
	pss_selectToKeep(buffer)
	if pss_debug then
		print_view("passiveThread ("..job.position.."): after selectToKeep")
	end
	no_passive_while_active_lock:unlock()
	return ret
end

function pss_activeThread()
	-- take a lock to prevent being called as a passive thread while
	-- on an exchange with another peer
	no_passive_while_active_lock:lock()
	-- select a partner
	partner_ind = pss_selectPartner()
	partner = view[partner_ind]
	-- remove the partner from the view
	table.remove(view,partner_ind)
	-- select what to send to the partner
	buffer = pss_selectToSend()
	if pss_debug then
		print_set_of_peers(buffer,"activeThread ("..job.position.."): sending to "..partner.id)
	end
	-- send to the partner
	local ok, r = rpc.acall(partner.peer,{"pss_passiveThread", job.position, buffer},pss_active_thread_period/2)
	if ok then
		-- select what to keep etc.
		local received = r[1]
		if pss_debug then
			print_set_of_peers(received,"activeThread ("..job.position.."): received from "..partner.id)
		end
		pss_selectToKeep(received)
		if pss_debug then
			print_view("activeThread ("..job.position.."): after selectToKeep")
		end
	else
		-- peer not replying? remove it from view!
		if pss_debug then
			log:print("on peer ("..job.position..") peer "..partner.id.." did not respond -- removing it from the view")
		end
		table.remove(view,partner_ind)
	end	
	-- all ages increment
	for _,v in ipairs(view) do
		v.age = v.age + 1
	end
	-- now, allow to have an incoming passive thread request
	no_passive_while_active_lock:unlock()
end

--[[
******************************************************************************
                            THE PEER SAMPLING API
******************************************************************************
]]

pss_initialized = false
function pss_init()
	-- ideally, would perform a random walk on an existing overlay
	-- but here we emerge from the void, so let's use the Splay provided peers
	-- note that we select randomly c+1 nodes so that if we have ourself in it,
	-- we avoid using it. Ages are taken randomly in [0..c] but could be 
	-- 0 as well.
	if #job.nodes <= c then
		log:print("There are not enough nodes in the initial array from splay.")
		log:print("Use a network of at least "..(c+1).." nodes, and an initial array of type random with at least "..(c+1).." nodes")
		log:print("FATAL: exiting")
		os.exit()
	end
	if H + S > c/2 then
		log:print("Incorrect parameters H = "..H..", S = "..S..", c = "..c)
		log:print("H + S cannot be more than c/2")
		log:print("FATAL: exiting")
		os.exit()
	end
	local indexes = {}
	for i=1,#job.nodes do
		indexes[#indexes+1]=i
	end
	local selected_indexes = misc.random_pick(indexes,c+1)	
	local i = 1
	while #view < c do
		if not (selected_indexes[i] == job.position) then
			view[#view+1] = 
			{peer={ip=job.nodes[selected_indexes[i]].ip,port=job.nodes[selected_indexes[i]].port},age=0,id=selected_indexes[i]}
		end
		i=i+1
	end
	assert (#view == c)
	if pss_debug then
		print_view("initial view")
	end
	-- from that time on, we can use the view.
	pss_initialized = true	

	math.randomseed(job.position*os.time())
	-- wait for all nodes to start up (conservative)
	events.sleep(2)
	-- desynchronize the nodes
	local desync_wait = (pss_active_thread_period * math.random())
	if pss_debug then
		log:print("waiting for "..desync_wait.." to desynchronize")
	end
	events.sleep(desync_wait)  

	for i =1, 4 do
		pss_activeThread()
		events.sleep(pss_active_thread_period / 4)
	end
	t1 = events.periodic(pss_activeThread,pss_active_thread_period)
end  

function pss_getPeer()
	if pss_initialized == false then
		log:print("Call to pss_getPeer() while the PSS is not initialized:")
		log:print("wait for some time before using the PSS!")
		log:print("FATAL. Exiting")
	end
	if #view == 0 then
		return nil
	end
	return view[math.random(#view)]
end


--[[
******************************************************************************
                           CLASSICAL CHORD SERVICE
******************************************************************************
]]--


n = {}
m = 32
num_successors = 8
tchord_debug = false
predecessor = nil
successors = {}
finger = {}
tchord_view = {}
tchord_active_thread_period = 20



--Getters and Setters for predecessor and successor:
function get_successor()
	return finger[1].node
end


function get_predecessor()
	return predecessor
end


function set_successor(node)
	finger[1].node = node
	if chord_debug then
		log:print("new successor = "..finger[1].node.id)
	end
end


function set_predecessor(node)
	predecessor = node
	if chord_debug then
		log:print("new predecessor = "..predecessor.id)
	end
end

--Verifies if ID is inside (low, high] (all numbers):
function does_belong_open_closed(id, low, high)
	if low < high then
	  return (id > low and id <= high)
	else
	  return (id > low or id <= high)
	end
end

--Verifies if ID is inside [low, high) (all numbers):
function does_belong_closed_open(id, low, high)
	if low < high then
	  return (id >= low and id < high)
	else
	  return (id >= low or id < high)
	end
end

--Verifies if ID is inside [low, high) (all numbers):
function does_belong_open_open(id, low, high)
	if low < high then
	  return (id > low and id < high)
	else
	  return (id > low or id < high)
	end
end

--Returns the finger with bigger i which is preceding a given ID
function closest_preceding_finger(id)
	--from m to 1:
	for i = m,1,-1 do
		--if the finger is between the node and the given ID:
		if does_belong_open_open(finger[i].node.id, n.id, id) then
			--return this finger:
			return finger[i].node
		end
	end
end

--Finds the predecessor of a given ID (number):
function find_predecessor(id)
	--Starts with itself:
	local node = n
	local node_succ = get_successor()
	--Initializes counter:
	local i = 0
	--Iterates while id does not belong to (node, node_succ]:
	while (not does_belong_open_closed(id, node.id, node_succ.id)) do
		--the successor substitutes the current node for the next iteration:
		node = rpc.call(node, {"closest_preceding_finger", id})
		--the successor's successor is the new successor in the next iteration:
		node_succ = rpc.call(node, "get_successor")
		--Increments counter:
		i = i + 1
	end
	return node, i
end

--Finds the successor of a given ID (number):
function find_successor(id)
	--Finds the predecessor:
	local node = find_predecessor(id)
	--Gets the successor of the resulting node:
	local node_succ = rpc.call(node, "get_successor")
	return node_succ
end

--Initializes the neighbors when a node is joining the Chord network:
function init_finger_table(node)
	--the first finger node is calculated by 'node' as the successor of finger1.start:
	finger[1].node = rpc.call(node, {"find_successor", finger[1].start})
	--for the other fingers:
	for i = 1, m-1 do
		--if finger[i].start is between the node's ID and the last calculated finger:
		if does_belong_closed_open(finger[i+1].start, n.id, finger[i].node.id) then
			--this finger is the same as last one:
			finger[i+1].node = finger[i].node
		else --if not,
			--finds successor with the help of 'node':
			finger[i+1].node = rpc.call(node, {"find_successor", finger[i + 1].start})
		end
	end
end

function update_finger_table(s, i)
	--local low = n.id --I REMOVED THIS, WHY IS THIS WAY IN THE ALGO?
	local low = finger[i].start --I ADDED THIS
	local high = finger[i].node.id --I ADDED THIS
	if low ~= high then --I ADDED THIS
		--if the node to be inserted is closer to finger[i].start than finger[i].node:
		if does_belong_closed_open(s.id, low, high) then
			--replaces the finger:
			finger[i].node = s
			--looks for the predecessor of this node:
			p = get_predecessor()
			--and updates also the node's predecessor if needed:
			rpc.call(p, {"update_finger_table", s, i})
		end
	end --I ADDED THIS
end


function update_others()
	--updates its successor of it being the new predecessor:
	rpc.call(get_successor(), {"set_predecessor", n})
	--updates the finger tables:
	for i = 1, m do
		local id = (n.id + 1 - 2^(i-1)) % 2^m --I ADDED THIS +1
		--finds the predecessor of n - 2^(i-1) (does backwards the calculation of start):
		local p = find_predecessor(id)
		--updates the finger table of this node:
		rpc.call(p, {"update_finger_table", n, i})
	end
end

--Joins the Chord network:
function join(node)
	--If a node is given as input:
	if node then
		--Initializes the neighbors through this node:
		init_finger_table(node)
		set_predecessor(rpc.call(get_successor(), "get_predecessor"))
		update_others()
	else
		--If not, you are alone in the network, so you are your own predecessor, successor and fingers:
		for i = 1, m do
			finger[i].node = n
		end
		set_predecessor(n)
	end
end

--Initializes IP, port, ID in variable n
function init_chord()
	predecessor = nil
	successors = {}
	for i = 1, m do
		finger[i] = {
			start = (n.id + 2^(i-1))%2^m
		}
	end

end


function print_chord()
	log:print("local_node: "..n.id)
	log:print("predecessor: "..predecessor.id)

	for i = 1, #successors do
		log:print("successor "..i.." : "..successors[i].id)
	end

	for i = 1, #finger do
		log:print("finger "..i..": start:"..finger[i].start..", node: "..finger[i].node.id)
	end
end


--[[
******************************************************************************
                         THIS IS WHERE YOUR CODE GOES
******************************************************************************
]]

-- computes the sha1 hash of a node and converts the hex output into a number
-- when not using multiples of 4 for m this results in a smaller hash space than expected
function compute_hash(node)
	local o = (node.ip..":"..tostring(node.port))
	return tonumber(string.sub(crypto.evp.new("sha1"):digest(o), 1, m / 4), 16)
end

function bootstrap_chord()
	n = {ip=job.me.ip,port=job.me.port,id=compute_hash(job.me)}
	logD("extracting chord links from tman view","bootstrap_chord")
	predecessor, successors, finger = extract_view(job_nodes_to_view())
	if debug and debug_level["bootstrap_chord"] then
		print_chord()
	end
end

function extract_view(view)
	local temp = misc.dup(view)
	logD("Sorting the tman view","extract_view")
	table.sort(temp,function(a,b) return ((a.id-n.id)%(2^m)) < ((b.id-n.id)%(2^m)) end)
	if debug and debug_level["extract_view"] then
		print_tman_table(temp)
	end
	local temp_predecessor = nil
	local temp_sucessors = {}
	local temp_fingers = {}
	logD("extracting predecessor","extract_view")
	temp_predecessor = temp[#temp]
	logD("extracting successors","extract_view")
	for i=2, num_successors+1 do
		table.insert(temp_sucessors, temp[i])
	end
	logD("generating finger starts","extract_view")
	for i = 1, m do
		temp_fingers[i] = {
			start = (n.id + 2^(i-1))%2^m
		}
	end
	logD("extracting fingers","extract_view")
	for i=1, #temp_fingers do
		for j=1, #temp do
			if temp_fingers[i].start < temp[j].id then
				temp_fingers[i].node = temp[j]
				break
			end
		end
	end
	return temp_predecessor, temp_sucessors, temp_fingers
end



function job_nodes_to_view()
	local temp = {}
	for i=1, #job.nodes do
		if not job.nodes[i].died then
			table.insert(temp,{ip=job.nodes[i].ip,port=job.nodes[i].port,id=compute_hash(job.nodes[i])})
		end
	end
	return temp
end
function print_tman_table(t)
	log:print("[ (size "..#t..")")
	for i=1,#t do
		log:print("  "..i.." : ".."["..t[i].ip..":"..t[i].port.."] - id: "..t[i].id)
	end
	log:print("]")
end

function compareViewsNumber(temp_predecessor, temp_sucessors, temp_fingers, predecessor, sucessors, fingers)
	local count = 0
	if not temp_predecessor.id == predecessor.id then
		count = count + 1
	end
	for i=1, #temp_sucessors do
		if not temp_sucessors[i] == sucessors[i] then
			count = count + 1
		end
	end
	for i=1, #temp_fingers do
		if not temp_fingers[i] == fingers[i] then
			count = count + 1
		end
	end
	return count
end

function terminator()
	log:print("node "..job.position.." will end in 10min")
	events.sleep(600)
	log:print("Terminator Exiting")
	os.exit()
end


function main ()
	events.sleep(7)
	bootstrap_chord()
	log:print(compareViewsNumber(predecessor, successors, finger,extract_view(job_nodes_to_view())))
	os.exit()
end

events.thread(main)  
events.loop()
