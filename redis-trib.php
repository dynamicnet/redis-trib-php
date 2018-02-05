#!/usr/bin/env php
<?php
define("CLUSTER_HASH_SLOTS_COUNT", 16384);
define("REBALANCE_DEFAULT_THRESHOLD", 2);


// A collection of "Node" objects
$CLUSTER = new Cluster();

function create_cluster_cmd($args, $opts){
	global $CLUSTER;

	$force_flush = isset($opts["force-flush"]);
	$simulate    = isset($opts["simulate"]);

	$master_nodes = [];


	foreach ($args as $a) {
		$n = new Node($a);
		$n->is_master = true;
		$master_nodes[] = $n;
	}


	if( count($master_nodes) < 2 ){
		print_log("You need at least 2 nodes");
		exit(1);
	}


	// Test node connectivity
	ping_all_nodes( $master_nodes );


	// Assert all nodes are cluster ready
	foreach( $master_nodes as $node ){
		$is_cluster_enabled = assert_cluster( $node );

		if( !$is_cluster_enabled ){
			print_log( "[ERR] Node {$node->addr}:{$node->port} is not configured as a cluster node.");
			exit(1);
		}
	}

	// Get nodes maxmemory (to calculate weight)
	foreach( $master_nodes as $node ){
		$maxmemory = node_get_maxmemory( $node );

		print_log( ">>> {$node->addr} memory {$maxmemory}");
		$node->maxmemory = $maxmemory;
	}

	// Dispatch slots per node (using maxmemory as weight factor)
		$memory_sum = 0;

		foreach( $master_nodes as $node ){
			$memory_sum = $memory_sum+(int)$node->maxmemory;
		}

		// How many slot to allocate per node
		$per_slot_memory = (CLUSTER_HASH_SLOTS_COUNT) / $memory_sum;
		$slot_index_current = -1;
		foreach( $master_nodes as $node ){
			$node_first_slot_index = $slot_index_current+1;

			$node_slot_count = round($per_slot_memory * $node->maxmemory);

			$node_last_slot_index = $node_first_slot_index+$node_slot_count-1;

			$node->node_first_slot_index = $node_first_slot_index;
			$node->node_last_slot_index = $node_last_slot_index;

			$slot_index_current = $node->node_last_slot_index;
		}


	// CLUSTER CONFIG RESET
		foreach( $master_nodes as $node ){
			$dbsize = _cmd($node, "DBSIZE");

			if( $dbsize > 0 ){
				if( $force_flush && !$simulate ){
					$cmd = _cmd($node, "FLUSHALL");
				} else {
					print_log("[ERR] Node $node already store some keys");
					exit(1);
				}
			}

			if( !$simulate ){
				$reply = _cmd($node, "CLUSTER", "RESET", "HARD");

				if( "OK" != $reply ){
					print_log( "[ERR] ".$reply );
					exit(1);
				}
			}
		}
		print_log("[OK] Nodes Hard Reset");

	// EPOCH ON EACH NODES
		print_log(">>> Set Config Epoch");

		foreach( $master_nodes as $node ){
			if( !$simulate ){
				assign_config_epoch( $node );
			}
		}

		print_log("[OK] Set Config Epoch");


	// Allocate slots to each node
		print_log(">>> Performing hash slots allocation on nodes...");

		foreach( $master_nodes as $node ){
			print_log(">>> Allocate slot range {$node->node_first_slot_index}-{$node->node_last_slot_index} to {$node}");

			if( !$simulate ){
				$args = array($node, "CLUSTER", "ADDSLOTS");
				$args = array_merge($args, range($node->node_first_slot_index, $node->node_last_slot_index));

				$reply = call_user_func_array("_cmd", $args);;

				if( "OK" != $reply ){
					print_log( "[ERR] ".$reply );
					exit(1);
				}
			}
		}

		print_log("[OK] Slots allocated to nodes");


	// each nodes join cluster
		print_log(">>> Sending CLUSTER MEET messages to join the cluster");

		$first = false;
		foreach( $master_nodes as $node ){
			if( !$first ){
				$first = $node;
				continue;
			}

			if( !$simulate ){
				$reply = _cmd($node, "CLUSTER", "MEET", $first->addr, $first->port);

				if( "OK" != $reply ){
					print_log( "[ERR] ".$reply );
				}
			}
		}


	if( !$simulate ){
		// Building the node collection
		// this is needed for wait_cluster_join
		$CLUSTER->reset();
		foreach( $master_nodes as $node ){
			$CLUSTER->add_node($node);
		}

		wait_cluster_join();

		// verify cluster
		$CLUSTER->reset();
		load_cluster_info_from_node( new Node($args[0]) );
		check_cluster();
	}
}

function fix_cluster_cmd($args, $opts){
	$opts["fix"] = true;

	$existing_cluster_node = new Node($args[0]);
	load_cluster_info_from_node($existing_cluster_node);
	check_cluster( $opts );
}

function info_cluster_cmd($args, $opts){
	$existing_cluster_node = new Node($args[0]);

	// Recherche les infos sur la composition de l'ensemble du cluster
	load_cluster_info_from_node($existing_cluster_node);

	show_cluster_info();
}

function rebalance_cluster_cmd($args, $opts){
	global $CLUSTER;

	$simulate = isset($opts["simulate"]);
	$useempty = isset($opts["use-empty-masters"]);
	$threshold = isset($opts["threshold"]) ? intval($opts["threshold"]) : REBALANCE_DEFAULT_THRESHOLD;

	$existing_cluster_node = new Node($args[0]);

	// Recherche les infos sur la composition de l'ensemble du cluster
	load_cluster_info_from_node($existing_cluster_node);

	// Options parsing
	$weights = [];
	if( isset($opts["weight"]) ){
		foreach( $opts["weight"] as $w ){
			$fields = explode("=", $w);

			$node = get_node_by_abbreviated_name($fields[0]);

			if( !$node || !$node->is_master ){
				echo "*** No such master node {$fields[0]}\n";
				exit(1);
			}

			$weights[$node->name] = intval($fields[1]);
		}
	}


	$MASTERS_NODES = $CLUSTER->get_masters();

	// Get nodes maxmemory (to calculate weight)
	// Dispatch slots per node (using maxmemory as weight factor)
		$memory_sum = 0;

		foreach( $MASTERS_NODES as $node ){
			if( ! $useempty && 0 == count($node->slots)){
				$maxmemory = 0;
			} elseif( isset($weights[$node->name]) ){
				$maxmemory = $weights[$node->name];
			} else {
				$maxmemory = node_get_maxmemory($node);
			}

			$memory_sum = $memory_sum+(int)$maxmemory;
			$node->maxmemory = $maxmemory;

		}


		// How many slot to allocate per node
		$per_slot_memory = (CLUSTER_HASH_SLOTS_COUNT-1) / $memory_sum;
		foreach( $CLUSTER->get_masters() as $node ){
			$node->target_slot_count = round($per_slot_memory * $node->maxmemory);
		}

		// Calculate the slots balance for each node. It's the number of
		// slots the node should lose (if positive) or gain (if negative)
		// in order to be balanced.
		$total_balance = 0;
		$has_negative_balance = false;
		$threshold_reached = false;

		foreach( $MASTERS_NODES as $node ){
			$balance = count($node->slots)-$node->target_slot_count;
			$node->balance = $balance;
			$total_balance = $total_balance+$balance;

			// Compute the percentage of difference between the
			// expected number of slots and the real one, to see
			// if it's over the threshold specified by the user.
			$over_threshold = false;

			if( $threshold > 0 ){
				if( count($node->slots) > 0 ){
					$err_perc = round(100-(100*$node->target_slot_count/count($node->slots)), 2);

					if(abs($err_perc) > $threshold){
						$over_threshold = true;
					}
				} elseif( $node->balance < 0 ){
					$over_threshold = true;
				}
			}

			if( $over_threshold ){
				$threshold_reached = true;
			}
		}

		if( false === $threshold_reached){
			print_log("*** No rebalancing needed! All nodes are within the {$threshold}% threshold.");
			exit(0)	;
		}


	// Because of rounding, it is possible that the balance of all nodes
	// summed does not give 0. Make sure that nodes that have to provide
	// slots are always matched by nodes receiving slots.
	if( $total_balance > 0 ){
		while($total_balance > 0){
			foreach( $MASTERS_NODES as $node ){
				if( $node->balance < 0 && $total_balance > 0 ){
					$node->balance--;
					$total_balance--;
				}
			}
		}
	}

	// Sort nodes by their slots balance.
	// lower balance first
	usort( $MASTERS_NODES, function($a, $b){
		return $a->balance - $b->balance;
	} );

	foreach( $MASTERS_NODES as $node ){
		print_log("{$node} balance is {$node->balance} slots");
	}


	# Now we have at the start of the 'sn' array nodes that should get
	# slots, at the end nodes that must give slots.
	# We take two indexes, one at the start, and one at the end,
	# incrementing or decrementing the indexes accordingly til we
	# find nodes that need to get/provide slots.

	$slots_movements = [];

	$dst_idx = 0;
	$src_idx = count($MASTERS_NODES) - 1;

	while($dst_idx < $src_idx){
		$dst = $MASTERS_NODES[$dst_idx];
		$src = $MASTERS_NODES[$src_idx];

		$numslots = min(abs($dst->balance), abs($src->balance));

		if( $numslots > 0 ){
			// Compute which slots are moving from src to dst
			$slots_movements[] = [
				"src"      => $src,
				"dst"      => $dst,
				"slots_to_move" => array_splice($src->slots, 0, $numslots)
			];
		}

		$dst->balance += $numslots;
		$src->balance -= $numslots;

		if( 0 == $dst->balance ){
			$dst_idx++;
		}

		if( 0 == $src->balance ){
			$src_idx--;
		}
	}

	/*foreach( $slots_movements as $sm ){
		echo $sm["src"]."\n";
		print_r($sm["slots_to_move"]);
	}exit;*/

	// So...moving slots
	foreach( $slots_movements as $sm ){
		$numslots = count($sm["slots_to_move"]);
		$src = $sm["src"];
		$dst = $sm["dst"];

		print_log(">>> Moving {$numslots} slots from {$src} to {$dst}");



		foreach( $sm["slots_to_move"] as $slot ){
			if( !$simulate ){
				move_slot(
					$src,
					$dst,
					$slot,
					$MASTERS_NODES,
					["quiet"=>true,
					"update"=>true]
				);

				echo "#";
			} else {
				echo str_repeat("#", $numslots);
			}
		}

		print_log("\n[OK] Moving {$numslots} slots.");
	}

}

function addnode_cluster_cmd($args, $opts){
	global $CLUSTER;

	$simulate = isset($opts["simulate"]);

	$existing_cluster_node = new Node($args[1]);
	$added_node = new Node($args[0]);

	print_log(">>> Adding node {$added_node} to cluster {$existing_cluster_node}");

	// Check the existing cluster
	load_cluster_info_from_node($existing_cluster_node);
	check_cluster();


	$cluster_ping = ping_node_isok( $existing_cluster_node );

	if( ! $cluster_ping ){
		print_log("[ERR] Existing cluster node {$existing_cluster_node} seems offline");
		exit(1);
	}

	$node_ping = ping_node_isok( $added_node );

	if( ! $node_ping ){
		print_log("[ERR] New node {$added_node} seems offline");
		exit(1);
	}


	// Assert the new node is cluster ready
	$is_cluster_enabled = assert_cluster( $added_node );

	if( !$is_cluster_enabled ){
		print_log( "[ERR] Node {$added_node} is not configured as a cluster node.");
		exit(1);
	}

	assert_empty( $added_node );

	// Add the new node to your Node collection
	$CLUSTER->add_node($added_node);

	// JOIN
	print_log(">>> Send CLUSTER MEET to new node to make it join the cluster.");

	if( !$simulate ){
		$reply = _cmd($added_node, "CLUSTER", "MEET", $existing_cluster_node->addr, $existing_cluster_node->port);

		if( "OK" != $reply ){
			print_log( "[ERR] ".$reply );
		}

		wait_cluster_join();
	}


	// verify cluster
	print_log("[OK] New node added correctly.");

}

function delnode_cluster_cmd($args, $opts){
	global $CLUSTER;

	$existing_cluster_node = new Node($args[0]);
	$nodeid_delete = $args[1];

	print_log(">>> Removing node {$nodeid_delete} from cluster {$existing_cluster_node}");

	load_cluster_info_from_node($existing_cluster_node);


	$node_delete = get_node_by_name($nodeid_delete);


	if( false === $node_delete ){
		print_log("[ERR] No such node ID {$node_delete}");
		exit(1);
	}

	if(count($node_delete->slots) != 0){
		print_log("[ERR] Node {$node_delete->name} is not empty! Reshard data away and try again.");
		exit(1);
	}

	# Send CLUSTER FORGET to all the nodes but the node to remove
	print_log(">>> Sending CLUSTER FORGET messages to the cluster...");

	foreach($CLUSTER->get_nodes() as $node){
		if( $node->name == $nodeid_delete ){ continue; }

		forget_node($node, $nodeid_delete);
	}

	print_log(">>> SHUTDOWN the node.");
	shutdown_node( $node_delete );
}

function check_cluster_cmd($args, $opts){
	$existing_cluster_node = new Node($args[0]);

	load_cluster_info_from_node($existing_cluster_node);
	check_cluster( $opts );
}

function call_cluster_cmd($args, $opts){
	global $CLUSTER;

	$cmd = array_slice($args, 1);

	# Load cluster information
	$a_cluster_node = new Node($args[0]);
	load_cluster_info_from_node($a_cluster_node);

	print_log(">>> Calling ".implode(" ", $cmd));
	foreach( $CLUSTER->get_nodes() as $n ){
		$args[0] = $n;
		$res = call_user_func_array("_cmd", $args);

		if( !is_string($res) ){
			$res = implode("\n", $res);
		}

		print("{$n}: {$res} \n\n");
	}
}

function help_cluster_cmd(){
	show_help();
}

/**
 *	Mostly, tests the
 */
function test_cmd(){
	$str_test = "219-802";
	list($slots, $migrating, $importing) = parse_slots_string( $str_test );

	if( 584 != count($slots) || 0!= count($migrating) || 0!= count($importing) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}



	$str_test = "219-802 4469-16383";
	list($slots, $migrating, $importing) = parse_slots_string( $str_test );

	if( 12499 != count($slots) || 0!= count($migrating) || 0!= count($importing) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}


	$str_test = "0-218 964-4468 [219-<-368115be096aef66d7a4f916712f6adaa1fb46ee] [4469-<-d0823a6a5bc6c398b9842c3c6bdab05d9aab6f11]";
	list($slots, $migrating, $importing) = parse_slots_string( $str_test );

	if( 3724 != count($slots) || 0!= count($migrating) || 2 != count($importing) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}

	if( !array_key_exists(219, $importing) || !array_key_exists(4469, $importing) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}


	$str_test = "0-218 964-4468 5000 [219-<-368115be096aef66d7a4f916712f6adaa1fb46ee] [4469-<-d0823a6a5bc6c398b9842c3c6bdab05d9aab6f11] [4470->-d0823a6a5bc6c398b9842c3c6bdab05d9aab6f11] [4471->-d0823a6a5bc6c398b9842c3c6bdab05d9aab6f11]";
	list($slots, $migrating, $importing) = parse_slots_string( $str_test );

	if( 3725 != count($slots) || 2!= count($migrating) || 2 != count($importing) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}

	if( !array_key_exists(4470, $migrating) || !array_key_exists(4471, $migrating) ){
		print_log("[ERR] Error parsing '{$str_test}'");
		exit(1);
	}


	$n = new Node();
	$n->add_slot(870);
	$n->add_slot(871);
	$n->add_slot(872);

	if( 3 != count($n->slots) ) {
		print_log("[ERR] Error adding slots to node object");
		exit(1);
	}

	$n->delete_slot(871);

	if( 2 != count($n->slots) ) {
		print_log("[ERR] Error removing slots from node object");
		exit(1);
	}	

	print_log("[OK] All tests are ok");
}



function parse_options($subcommand){
	global $argv,$argc,$ALLOWED_OPTIONS;

	// 0 is the filename redis-trib.php
	// 1 is the subcommand name
	// so parsing option begin at idx 2
	$idx = 2;

	$options = [];

	while( $idx < $argc && "--" == substr( $argv[$idx],0,2 ) ){
		if( "--" == substr( $argv[$idx],0,2 ) ){
			$option = substr($argv[$idx], 2);
			$idx++;
			// --verbose is a global option
			if("verbose" == $option){
				$options["verbose"] = true;
				continue;
			}

			// --quiet is a global option
			if("quiet" == $option){
				$options["quiet"] = true;
				continue;
			}

			if( !isset($ALLOWED_OPTIONS[$subcommand]) || !isset($ALLOWED_OPTIONS[$subcommand][$option]) ){
				echo "Unknown option '{$option}' for command '{$subcommand}'\n";
				exit(1);
			}

			if( false != $ALLOWED_OPTIONS[$subcommand][$option] ){
				$value = $argv[$idx];
				$idx++;
			} else {
				$value = true;
			}


			# If the option is set to [], it's a multiple arguments
			# option. We just queue every new value into an array.
			if( $ALLOWED_OPTIONS[$subcommand][$option] === "[]" ){
				if( !isset($options[$option]) ){ $options[$option] = []; }
				$options[$option][] = $value;
			} else {
				$options[$option] = $value;
			}
		} else {
			// Remaining arguments are not options.
			break;
		}
	}

	// Enforce mandatory options
	if( isset($ALLOWED_OPTIONS[$subcommand]) ){
		foreach( $ALLOWED_OPTIONS[$subcommand] as $option=>$val ){
			if( !isset($options[$option]) && REQUIRED === $val ){
				echo "Option '--{$option}' is required for subcommand '{$subcommand}'\n";
				exit(1);
			}
		}
	}

	return [$options,$idx];
}


function check_arity($req_args, $num_args){
	if(
		($req_args > 0 and $num_args != $req_args) ||
		($req_args < 0 and $num_args < abs($req_args))
	){

		print_log("[ERR] Wrong number of arguments for specified sub command");
		exit(1);
	}
}


function print_log( $msg ){
	$color = null;

	switch( substr($msg,0,3) ){
		case '>>>':
			$color ="29;1";
		break;
		case '[ER':
			$color ="31;1";
		break;
		case '[WA':
			$color ="31;1";
		break;
		case '[OK':
			$color ="32";
		break;
		case '***':
			$color ="33";
		break;
	}

	if($color){
		print("\033[{$color}m");
	}

	print($msg);

	if($color){
		print("\033[0m");
	}

	print("\n");
}


function is_config_consistent(){
	global $CLUSTER;

	$signatures = array();

	foreach( $CLUSTER->get_masters() as $node ){
		$signatures[] = get_config_signature($node);
	}

	return count(array_unique($signatures)) == 1;
}


# Return a single string representing nodes and associated slots.
function get_config_signature($node){
	$config = array();

	$reply = _cmd($node, "CLUSTER", "NODES");

	$lines = explode("\n", $reply);

	foreach( $lines as $line ){
		$s = explode(" ", $line);

		$slots_str = "";

		if( isset($s[8]) ){ // during a "wait_cluster_join" $s[8] may be inexistent
			// Remove migrations infos
			$slots_str = preg_replace("/\[.*\]/U", "", $s[8]);
		}

		$config[$s[0]] = array(
			"id"     => $s[0],
			"slots"  => $slots_str
		);
	}

	sort($config);

	return md5(json_encode($config));
}


function check_config_consistency(){
	if( is_config_consistent() ){
		print_log("[OK] All nodes agree about slots configuration.");
	} else {
		print_log("[ERR] Nodes don't agree about configuration!");
		exit;
	}
}


function covered_slots(){
	global $CLUSTER;

	$slots = array();

	$node = $CLUSTER->get_masters()[0];

	$reply = _cmd($node, "CLUSTER", "NODES");

	$lines = explode("\n", $reply);

	foreach( $lines as $line ){
		$s = explode(" ", $line);
		$slots_informations_str = implode(" ", array_slice($s, 8));
		list($s,,) = parse_slots_string($slots_informations_str);
		$slots = array_merge($slots , $s);
	}

	return $slots;
}


// always return an array
// ex:
// 1
// 0-253
// 1 0-253 1025 1026-1030
// 1 0-253 1025 1026-1030 [93-<-292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f]
// 1 0-253 1025 1026-1030 [93-<-292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f] [77->-e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca]
function parse_slots_string( $slots_str ){
	$slots     = [];
	$migrating = [];
	$importing = [];

	foreach(explode(" ", $slots_str) as $info ){
		$info = trim($info);
		if( "" == $info){
			continue;
		}

		if( 0 === strpos($info, "[") ){
			$info = str_replace(["[","]"],"", $info);

			if( false !== strpos($info, "-<-") ){ // Importing
				list($slot,$src) = explode("-<-", $info);
				$importing[$slot] = $src;

			} elseif( false !== strpos($info, "->-") ){ // Migrating

				list($slot,$dst) = explode("->-", $info);
				$migrating[$slot] = $dst;
			}
		} elseif( false !== strpos($info, "-") ){ // Range
			list($start,$end) = explode("-", $info);

			$slots = array_merge($slots, range($start, $end));
		} else { // Single slot number
			$slots[] = $info;
		}
	}

	return [
		$slots,
		$migrating,
		$importing
	];
}


function check_slots_coverage(){
	print_log(">>> Check slots coverage...");

	$slots = covered_slots();

	if(CLUSTER_HASH_SLOTS_COUNT == count($slots)){
		print_log("[OK] All ".CLUSTER_HASH_SLOTS_COUNT." slots covered.");
	} else {
		print_log("[ERR] Not all ".CLUSTER_HASH_SLOTS_COUNT." slots are covered by nodes.");
		exit;
	}
}


function assign_config_epoch(Node $node){
	$config_epoch = time();

	_cmd($node, "CLUSTER", "SET-CONFIG-EPOCH", $config_epoch);
}


function ping_all_nodes( $nodes ){
	print_log(">>> Ping all nodes to ensure there are online");

	foreach( $nodes as $node ){
		if( ! ping_node_isok( $node ) ){
			print_log( "[ERR] ".$node->addr.":".$node->port." was offline or firewalled ?" );
			exit;
		}
	}

	print_log("[OK] All nodes are online");
}


function ping_node_isok( Node $node ){
	$reply = _cmd($node, "PING");

	if( "PONG" != trim($reply) ){
		return false;
	}

	return true;
}


function node_get_maxmemory(Node $node){
	$reply = _cmd($node, "CONFIG", "GET", "maxmemory");

	if( !is_array($reply) ){
		$reply = explode("\n", $reply);
	}

	$maxmemory = (int)$reply[1];

	return $maxmemory;
}


function assert_cluster(Node $node){
	$reply = _cmd($node, "INFO", "CLUSTER");

	return 1==preg_match("/cluster_enabled\:1/", $reply);
}


// Load cluster typology from a member node
function load_cluster_info_from_node($node){
	global $CLUSTER;

	$CLUSTER->reset();
	$reply = _cmd($node, "CLUSTER", "NODES");

	$lines = explode("\n", $reply);

	foreach( $lines as $line ){
		$s = explode(" ", $line);

		// handle ipv6 and ipv4 + port
		list($addr, $port) = addr_and_port($s["1"]);

		$info = new Node;
		$info->rawstr = $line;
		$info->name = $s[0];
		$info->addr = $addr;
		$info->port = $port;

		if( preg_match("/master/", $s[2]) ){
			$info->is_master = true;
		}

		if( preg_match("/slave/", $s[2]) ){
			$info->is_slave = true;
		}

		// Get DB size
		$info->dbsize = _cmd($info, "DBSIZE");

		$CLUSTER->add_node($info);
	}


	// We need to get slots from each node
	// Because open (importing/migrating) slots only appear on the "myself" node
	foreach( $CLUSTER->get_masters() as $node ){
		$reply = _cmd($node, "CLUSTER", "NODES");

		$lines = explode("\n", $reply);

		foreach( $lines as $line ){
			$s = explode(" ", $line);

			if( ! preg_match("/myself/", $s[2]) ){
				continue;
			}

			if( isset($s[8]) ){
				$slots_informations_str = implode(" ", array_slice($s, 8));
				list($slots, $migrating, $importing) = parse_slots_string($slots_informations_str);
				$node->slots     = $slots;
				$node->migrating = $migrating;
				$node->importing = $importing;
			}
		}
	}
}


function show_cluster_info(){
	global $CLUSTER;

	$lines = [];

	$slots_count = 0;
	$keys_count = 0;

	foreach( $CLUSTER->get_nodes() as $i ){
		$line = [];

		$line[] = str_pad($i->addr.":".$i->port, 22, " ", STR_PAD_RIGHT);
		$line[] = $i->name;
		$line[] = str_pad($i->dbsize." keys", 11, " ", STR_PAD_LEFT);
		$line[] = str_pad(count($i->slots)." slots", 11, " ", STR_PAD_LEFT);
		$line[] = "migrating ".count($i->migrating)."(". implode(",", array_keys($i->migrating)) .")";
		$line[] = "importing ".count($i->importing)."(". implode(",", array_keys($i->importing)) .")";

		$slots_count += count($i->slots);
		$keys_count += $i->dbsize;

		$lines[] = implode(" | ", $line);
	}

	$lines[] = "{$keys_count} keys";

	if( $slots_count == CLUSTER_HASH_SLOTS_COUNT ){
		$lines[] = "[OK] All ".CLUSTER_HASH_SLOTS_COUNT." slots covered.";
	} else {
		$lines[] = "[ERR] Seems that all ".CLUSTER_HASH_SLOTS_COUNT." are not covered.";
	}

	array_walk($lines, "print_log");
}


// Move slots between source and target nodes using MIGRATE.
//
// Options:
// cold    -- Move keys without opening slots / reconfiguring the nodes.
// update  -- Update node->slots for source/target nodes.
// quiet   -- Don't print info messages.
function move_slot( $src, $dst, $slot, $nodes, $opts=[]){
	$cold   = ( isset($opts["cold"])&&$opts["cold"] );
	$fix    = ( isset($opts["fix"])&&$opts["fix"] );
	$quiet  = ( isset($opts["quiet"])&&$opts["quiet"] );
	$update = ( isset($opts["update"])&&$opts["update"] );

	if( ! $quiet ){
		print_log("Moving slot {$slot} from {$src} to {$dst}");
	}

	if( ! $cold ){
		// set importing
			$r = _cmd($dst, "CLUSTER", "SETSLOT", $slot, "IMPORTING", $src->name);
			if("OK" != $r){print_log("[ERR][".__LINE__."] $r");exit(1);}

		// set migrating
			$r = _cmd($src, "CLUSTER", "SETSLOT", $slot, "MIGRATING", $dst->name);
			if("OK" != $r){print_log("[ERR][".__LINE__."] $r");exit(1);}
	}



	$pipeline = 1;

	while( true ){
		$keys = get_keysinslot($src, $slot, $pipeline);

		if( 0 == count($keys) ){
			break;
		}

		//$r = _cmd($src, "MIGRATE", $dst->addr, $dst->port, "", 0, 10000, "KEYS", implode(" ", $keys));
		$r = _cmd($src, "MIGRATE", $dst->addr, $dst->port, $keys[0],0, 10000);

		if("OK" != $r && "NOKEY" != $r){ //  NOKEY isn't an error, according to official documentation
			print_log("[ERR][".__LINE__."] $r");exit(1);
		}


		// Update the node logical config
		if( $update ){
			$src->delete_slot($slot);
			$dst->add_slot($slot);
		}
	}

	// Set the new node as the owner of the slot in all the known nodes.
	if( !$cold ){
		foreach( $nodes as $node ){
			$r = _cmd($node, "CLUSTER", "SETSLOT", $slot, "NODE", $dst->name);
			if("OK" != $r){print_log("[ERR][".__LINE__."] $r");exit(1);}
		}
	}
}


function get_node_by_name($nodeid){
	global $CLUSTER;

	foreach( $CLUSTER->get_nodes() as $node ){
		if($node->name == $nodeid){
			return $node;
		}
	}

	return false;
}


function get_node_by_abbreviated_name($partial_node_id){
	global $CLUSTER;

	$candidates = [];

	foreach( $CLUSTER->get_nodes() as $node ){
		if(1==preg_match("/^".$partial_node_id."/i", $node->name)){
			$candidates[] = $node;
		}
	}

	if( count($candidates) == 1 ){
		return $candidates[0];
	}

	return false;
}


function forget_node($node, $nodeid_delete){
	$r = _cmd($node, "CLUSTER", "FORGET", $nodeid_delete);

	if("OK" != $r){
		print_log("[ERR] $r");
		exit(1);
	}

	return true;
}


function shutdown_node( Node $node ){
	$r = _cmd($node, "SHUTDOWN");

	if("" != $r){
		print_log("[ERR] $r");
		exit(1);
	}

	return true;
}


// handle ipv6 and ipv4 + port
function addr_and_port( $str ){
	$tmp = explode(":" , $str);
	$port = array_pop($tmp);
	$addr = implode(":", $tmp);

	return [$addr, $port];
}


function check_cluster( $opts = [] ){
	global $CLUSTER;

	$node = $CLUSTER->nodes[0];
	print_log(">>> Performing Cluster Check (using node {$node})");

	check_config_consistency();
	check_open_slots( $opts );
	check_slots_coverage();
}


function check_open_slots( $opts ){
	global $CLUSTER;

	$fix = isset($opts["fix"]);

	print_log(">>> Check for open slots...");

	$open_slots = [];

	foreach($CLUSTER->get_masters() as $node){
		if( count($node->migrating) > 0 ){

			$open_slots = array_merge($open_slots, array_keys($node->migrating));

			print_log("[WARNING] Node {$node->addr}:{$node->port} has slots in migrating state.");
		}

		if( count($node->importing) > 0 ){
			$open_slots = array_merge($open_slots, array_keys($node->importing));

			print_log("[WARNING] Node {$node->addr}:{$node->port} has slots in importing state.");
		}
	}


	$open_slots = array_unique($open_slots);

	if( count($open_slots) > 0 ){
		print_log("[WARNING] The following slots are open: ".implode("," , $open_slots));
	}

	if( $fix ){
		foreach($open_slots as $slot){
			fix_open_slot( $slot );
		}
	}
}


function fix_open_slot( $slot ){
	global $CLUSTER;

	print_log(">>> Fixing open slot {$slot}");

	# Try to obtain the current slot owner, according to the current
	# nodes configuration.
	$owners = get_slot_owners($slot);

	$owner = false;
	if( count($owners) > 0 ){
		$owner = $owners[0];
	}

	$migrating = [];
	$importing = [];
	foreach($CLUSTER->get_masters() as $node){
		if( isset($node->migrating[$slot]) ){
			$migrating[$node->name] = $node;
		} elseif( isset($node->importing[$slot]) ){
			$importing[$node->name] = $node;
		} elseif( get_countkeysinslot($node, $slot) > 0 && $node->name != $owner->name ){
			print_log("*** Found keys about slot {$slot} in node {$node}!");
			$importing[$node->name] = $node;
		}
	}

	print_log("Set as migrating in: ".implode(",", $migrating));
	print_log("Set as importing in: ".implode(",", $importing));

	if( false == $owner ){
		print_log(">>> Nobody claims ownership, selecting an owner...");

		$owner = get_node_with_most_keys_in_slot($CLUSTER->get_masters(),$slot);

		// If we still don't have an owner, we can't fix it.
		if( false == $owner ){
			print_log("[ERR] Can't select a slot owner. Impossible to fix.");
			exit(1);
		}

		// Use ADDSLOTS to assign the slot.
		print_log("*** Configuring {$owner} as the slot owner");

		_cmd($owner, "CLUSTER", "SETSLOT", $slot, "STABLE");
		_cmd($owner, "CLUSTER", "ADDSLOTS" , $slot);

		// Make sure this information will propagate. Not strictly needed
		// since there is no past owner, so all the other nodes will accept
		// whatever epoch this node will claim the slot with.
		_cmd($owner, "CLUSTER", "BUMPEPOCH");

		// Remove the owner from the list of migrating/importing nodes.
		unset($migrating[$owner->name]);
		unset($importing[$owner->name]);
	}


	// If there are multiple owners of the slot, we need to fix it
	// so that a single node is the owner and all the other nodes
	// are in importing state. Later the fix can be handled by one
	// of the base cases above.
	//
	// Note that this case also covers multiple nodes having the slot
	// in migrating state, since migrating is a valid state only for
	// slot owners.
	if(count($owners) > 1){
		print_log(">>> many owners");
		$owner = get_node_with_most_keys_in_slot($CLUSTER->get_masters(),$slot);

		foreach( $owners as $node ){
			if( $owner->name == $node->name ){
				continue;
			}

			_cmd($node, "CLUSTER", "DELSLOTS", $slot);

			_cmd($node, "CLUSTER", "SETSLOT", $slot, "IMPORTING", $owner->name);

			$importing[$node->name] = $node;

			_cmd($owner, "CLUSTER", "BUMPEPOCH");
		}
	}



	// Case 1: The slot is in migrating state in one slot, and in
	//         importing state in 1 slot. That's trivial to address.
	if( count($migrating) == 1 && count($importing) == 1){
		print_log(">>> Case 1");

		move_slot(
			reset($migrating),
			reset($importing),
			$slot,
			$CLUSTER->get_masters()
		);
	}



	// Case 2: There are multiple nodes that claim the slot as importing,
	// they probably got keys about the slot after a restart so opened
	// the slot. In this case we just move all the keys to the owner
	// according to the configuration.

	elseif( count($migrating) == 0 && count($importing) > 0 ){
		print_log(">>> Moving all the {$slot} slot keys to its owner {$owner}");

		foreach($importing as $node){
			if( $node->name == $owner->name ){ continue; }

			move_slot(
				$node,
				$owner,
				$slot,
				$CLUSTER->get_masters(),
				["cold"=>true]
			);

			print_log(">>> Setting {$slot} as STABLE in {$node}");

			_cmd($node, "CLUSTER", "SETSLOT", $slot, "STABLE");
		}
	}

	// Case 3: There are no slots claiming to be in importing state, but
	// there is a migrating node that actually don't have any key. We
	// can just close the slot, probably a reshard interrupted in the middle.
	elseif( count($importing) == 0 && count($migrating) == 1 && get_countkeysinslot(reset($migrating) , $slot) == 0){

		$node = reset($migrating);
		_cmd($node, "CLUSTER", "SETSLOT", $slot, "STABLE");
	}

	// Case 4: many nodes claims a slot, but only have one migrating
	// we assume that it's a very bad interrupted rebalance/reshard
	// so we move back the slot to the current owner (the migrating one)
	elseif( count($importing) > 1 && count($migrating) == 1){
		$receiving_node = reset($migrating);

		print_log(">>> Moving all the {$slot} slot keys to its owner {$receiving_node}");

		foreach($importing as $node){
			move_slot(
				$node,
				$receiving_node,
				$slot,
				$CLUSTER->get_masters(),
				["cold"=>true]
			);

			print_log(">>> Setting {$slot} as STABLE in {$node}");

			_cmd($node, "CLUSTER", "SETSLOT", $slot, "STABLE");
		}

		//Finally we set the slot as stable in the owner
		_cmd($receiving_node, "CLUSTER", "SETSLOT", $slot, "STABLE");

	}else {
		$migrating_in = implode(",", $migrating);
		$importing_in = implode(",", $importing);

		print_log("[ERR] Sorry, Redis-trib can't fix this slot yet (work in progress). Slot is set as migrating in {$migrating_in}, as importing in {$importing_in}, owner is {$owner}");
	}
}


// Return the node, among 'nodes' with the greatest number of keys
// in the specified slot.
function get_node_with_most_keys_in_slot($nodes, $slot){

	$best = false;

	$best_numkeys = 0;

	foreach( $nodes as $node ){
	$numkeys = get_countkeysinslot($node, $slot);

	if( $numkeys > $best_numkeys || false === $best ){
			$best = $node;
			$best_numkeys = $numkeys;
		}
	}

	return $best;
}


// Return the owner of the specified slot
function get_slot_owners($slot){
	global $CLUSTER;

	$owners = [];

	foreach( $CLUSTER->get_masters() as $node ){
		if( in_array($slot, $node->slots) ){
			$owners[] = $node;
		}
	}

	return $owners;
}


function get_countkeysinslot($node, $slot){
	$r = _cmd($node, "CLUSTER", "COUNTKEYSINSLOT", $slot);

	$keys_count = intval((string)$r);

	return $keys_count;
}


function get_keysinslot( $node, $slot, $count ){
	$keys = _cmd($node, "CLUSTER", "GETKEYSINSLOT", $slot, $count);

	if( !is_array($keys) ){
		$keys = explode("\n", $keys);
	}

	return array_filter($keys);
}


function assert_empty( $node ){
	$node_cluster_info = _cmd($node, "CLUSTER", "INFO");
	$dbsize = _cmd($node, "DBSIZE");

	if( $dbsize > 0 || ! preg_match("/cluster_known_nodes:1/", $node_cluster_info) ){
		print_log("[ERR] Node {$node} is not empty. Either the node already knows other nodes (check with CLUSTER NODES) or contains some key in database 0.");
		exit(1);
	}
}


function wait_cluster_join(){
	print("Waiting for the cluster to join");

	while( ! is_config_consistent() ){
		print(".");
		sleep(1);
	}

	print("\n");
}


function _cmd(){
	$argv = func_get_args();
	$node = $argv[0];

	$c = new RedisClient( $node->addr, $node->port );

	$args = array_slice($argv, 1);

	$return = $c->cmd($args);

	// reissue command on another host
	if( RedisClient::is_move($return) ){
		list(, $addr, $port) = RedisClient::extract_moved_info($return);

		$argv[0] = new Node($addr.":".$port);

		$return = call_user_func_array("_cmd", $argv);
	}

	return $return;
}


/**
*	Mostly insipired from https://github.com/ziogas/PHP-Redis-implementation
*/
class RedisClient {
	const INTEGER = ':';
	const INLINE = '+';
	const BULK = '$';
	const MULTIBULK = '*';
	const ERROR = '-';
	const NL = "\r\n";

	private $handle = false;

	public function __construct($host, $port, $timeout = 60){
		$this->host = $host;
		$this->port = $port;
		$this->handle = @fsockopen($host, (int)$port, $errno, $errstr, 5);

		if(false === $this->handle || $errno > 0){
			print_log("[ERR] Sorry, can't connect to node {$host}:{$port}");
			exit(1);
		}

		if (is_resource($this->handle)) {
			stream_set_timeout($this->handle, $timeout);
		}
	}

	public function __destruct(){
		if (is_resource($this->handle)) {
			fclose($this->handle);
		}
	}

	public function cmd($args){
		if (!$this->handle) {
			return $this;
		}

		$output = ['*'.count($args)];
		foreach( $args as $a ){
			$output[] = '$'.strlen($a);
			$output[] = $a;
		}

		$command = implode("\r\n", $output)."\r\n";

		fwrite($this->handle, $command);
		return $this->get_response();
	}

	protected function inline_response(){
		return trim(fgets($this->handle));
	}

	protected function integer_response(){
		return intval(trim(fgets($this->handle)));
	}

	protected function bulk_response(){
		$return = trim(fgets($this->handle));

		if ($return === '-1') {
			$return = null;
		} else {
			$return = $this->read_bulk_response($return);
		}

		return trim($return);
	}

	private function read_bulk_response($tmp){
		$response = null;
		$read = 0;
		$size = ((strlen($tmp) > 1 && substr($tmp, 0, 1) === self::BULK) ? substr($tmp, 1) : $tmp);
		while ($read < $size) {
			$diff = $size - $read;
			$block_size = $diff > 8192 ? 8192 : $diff;
			$chunk = fread($this->handle, $block_size);
			if ($chunk !== false) {
				$chunkLen = strlen($chunk);
				$read += $chunkLen;
				$response .= $chunk;
			} else {
				fseek($this->handle, $read);
			}
		}
		fgets($this->handle);
		return $response;
	}

	protected function multibulk_response(){
		$size = trim(fgets($this->handle));
		$return = false;

		if ($size === '-1') {
			$return = null;
		} else {
			$return = array();
			for ($i = 0; $i < $size; $i++) {
				$return[] = $this->get_response();
			}
		}

		return $return;
	}

	private function error_response(){
		$error = fgets($this->handle);

		// Returns error as string
		return trim($error);
	}


	protected function get_response(){
		$char = fgetc($this->handle);

		$return = false;

		switch ($char) {
			case self::INLINE:
				$return = $this->inline_response();
				break;
			case self::INTEGER:
				$return = $this->integer_response();
				break;
			case self::BULK:
				$return = $this->bulk_response();
				break;
			case self::MULTIBULK:
				$return = $this->multibulk_response();
				break;
			case self::ERROR:
				$return = $this->error_response();
				break;
		}

		return $return;
	}

	public static function is_move( $str ){
		return is_string($str)&&preg_match("/MOVED [0-9+]/", $str);
	}

	public static function extract_moved_info( $str ) {
		list(,$slot,$addr_and_port) = explode(" ", trim($str));

		list($addr, $port) = explode(":", $addr_and_port);

		return [$slot,$addr,(int)$port];
	}
}


class Node {
	public $rawstr;
	public $name;
	public $addr;
	public $port;
	public $dbsize;
	public $slots = [];
	public $migrating = [];
	public $importing = [];
	public $is_master=false;
	public $is_slave=false;

	public function __construct($addr_port=null){
		if( !is_null($addr_port) ){
			list($this->addr, $this->port) = addr_and_port($addr_port);
		}
	}

	public function __tostring(){
		return $this->addr.":".$this->port;
	}

	public function delete_slot($slot){
		if (($key = array_search($slot, $this->slots)) !== false) {
			unset($this->slots[$key]);
		}
	}
	
	public function add_slot($slot){	
		$this->slots[] = $slot;
	}
}


class Cluster {
	public $nodes = [];

	public function add_node( Node $node ){
		$this->nodes[] = $node;
	}

	public function get_nodes(){
		return $this->nodes;
	}

	public function get_masters(){
		$masters = [];
		foreach( $this->nodes as $n ){
			if( $n->is_master ){
				$masters[] = $n;
			}
		}

		return $masters;
	}

	public function reset(){
		$this->nodes = [];
	}
}


//--------------------------------------------------------
// Definition of commands
//--------------------------------------------------------

$COMMANDS=[
	"create"    => ["create_cluster_cmd", -2, "host1:port1 ... hostN:portN"],
	"check"     => ["check_cluster_cmd", 1, "host:port"],
	"info"      => ["info_cluster_cmd", 1, "host:port"],
	"fix"       => ["fix_cluster_cmd", 1, "host:port"],
	"rebalance" => ["rebalance_cluster_cmd", 1, "host:port"],
	"add-node"  => ["addnode_cluster_cmd", 2, "new_host:new_port existing_host:existing_port"],
	"del-node"  => ["delnode_cluster_cmd", 2, "host:port node_id"],
	"call"      => ["call_cluster_cmd", -2, "host:port command arg arg .. arg"],
	"help"    => ["help_cluster_cmd", 0, "(show this help)"],
	"test"    => ["test_cmd", 0, "Launch test"],
];


define('REQUIRED', "yes");
$ALLOWED_OPTIONS=[
	"create"     => ["simulate" => false, "force-flush" => false],
	"add-node"   => ["simulate" => false],
	"rebalance"  => ["simulate" => false, "weight" => "[]", "use-empty-masters"=>false, "threshold" => true]
];

function show_help(){
	global $COMMANDS, $ALLOWED_OPTIONS;
	echo "Usage:\n  redis-trib.php <command> <options> <arguments ...>\n\n";

	echo "Availables commands:\n";
	foreach ($COMMANDS as $subcommand => $v) {
		echo "  {$subcommand} {$v[2]}";

		if( isset($ALLOWED_OPTIONS[$subcommand]) ){
			foreach($ALLOWED_OPTIONS[$subcommand] as $optname => $has_arg) {
				echo " --{$optname}";
				echo $has_arg ? " <arg>" : "";
			}
		}

		echo "\n";
	}

	echo "\nFor info, rebalance, del-node you can specify the host and port of any working node in the cluster.\n";
}


// Sanity check
if( 1 == $argc){
	show_help();
	exit(1);
}

$subcommand = strtolower($argv[1]);

if( ! isset($COMMANDS[$subcommand]) ){
	echo "Unknown redis-trib subcommand '{$subcommand}'\n";
	exit(1);
}

$subcommand_fct = $COMMANDS[$subcommand][0];

list($cmd_options,$first_non_option) = parse_options($subcommand);
check_arity($COMMANDS[$subcommand][1], $argc-($first_non_option));


$subcommand_fct( array_slice($argv,$first_non_option) ,$cmd_options );
