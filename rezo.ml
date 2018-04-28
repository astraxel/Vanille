module type S = sig
  type flag
  type 'a process
  type 'a in_port
  type 'a out_port

  val new_channel: unit -> 'a in_port * 'a out_port
  val put: ?flags: flag list -> 'a -> 'a out_port -> unit process
  val get: ?flags: flag list -> 'a in_port -> 'a process

  val doco: unit process list -> unit process

  val return: ?flags: flag list -> 'a -> 'a process
  val bind: 'a process -> ('a -> 'b process) -> 'b process

  val run: 'a process -> 'a
end
              
module Lib (K : S) = struct

  let ( >>= ) x f = K.bind x f

  let delay f x =
    K.bind (K.return ()) (fun () -> K.return (f x))

  let par_map f l =
    let rec build_workers l (ports, workers) =
      match l with
      | [] -> (ports, workers)
      | x :: l ->
          let qi, qo = K.new_channel () in
          build_workers
            l
            (qi :: ports,
             ((delay f x) >>= (fun v -> K.put v qo)) :: workers)
    in
    let ports, workers = build_workers l ([], []) in
    let rec collect l acc qo =
      match l with
      | [] -> K.put acc qo
      | qi :: l -> (K.get qi) >>= (fun v -> collect l (v :: acc) qo)
    in
    let qi, qo = K.new_channel () in
    K.run
      ((K.doco ((collect ports [] qo) :: workers)) >>= (fun _ -> K.get qi))

end
              
module Th: S = struct
  
  type flag = MACHINE of int
            (* Les processus avec le flag MACHINE x identiques seront exécutés sur la même machine.
               Note : Dans le cas d'un bind, le flag doit être précisé sur le premier processus.
               Cela permet de maintenir des processus communiquant entre eux sur la même machine,
               et donc de réduire les communications réseau.
             *)
            
  type 'a process = { proc : (unit -> 'a);
                      flags : flag list;
                      mutable id : int;
                    }
                    (* Chaque process aura un unique moyen pour communiquer avec le processus
                       principal de la machine qui s'occupe de la communication avec le serveur.
                       Aucun input/output n'est crée si le process n'est pas encore affecté
                       ou si le flag NO_COMMUNICATION est spécifié.
                       Cf variables globales input et output.

                       Pour gérer les questions de doco, chaque processus aura un id. 
                       Il est à -1 initialement, et est modifié par le serveur
                     *)


                  

  (* TYPES DE COMMUNICATION *)
                  
  type 'a port = int           
  type 'a in_port = 'a port
  type 'a out_port = 'a port
  (*Le client assurera la communication avec le serveur, une pipe étant identifiée uniquement par
    un numéro.
    Outre le respect de la signature, le 'a indiquera les types au module Marshall.
    Ça marche au typage ?*)
                   
  
  type 'a data = { target : int ;
                   data : 'a;
                 }

  type doco_request = { content : unit process list;
                        target : int; (*id du processus hebergeant le doco*)
                      }

  type 'a communication = DC_QUERY
                        | DC_CONFIRM (*Message sent by servor to disconnect client*)
                        | CHANNEL_QUERY
                        | CHANNEL of int
                        | DATA of 'a data
                        | DATA_REQUEST of 'a port
                        | DOCO of doco_request
                        | DOCO_CONFIRM of int (*Message du serveur à un processus signalant que
                                                les processus parallèles sont terminés*)
                        | EXECUTE of 'a process
                        | RESULT of 'a data (*ici, le target est le numéro du processus
                                              envoyant le résultat*)


  (* TYPES SERVEUR *)
  type doco_count = { mutable current : int;
                      total : int;
                      client : int;
                    }

  type client_state = { mutable load : int;
                        mutable connected : bool; (*connected = false => le client a initialisé
                                                    la procédure de déconnexion*)
                      }

  type pipe = { input : Unix.file_descr;
                output : Unix.file_descr
              }

  type pipe_request = { pipe_id : int;
                        client_id : int
                      }
                           
                                

  (*VARIABLES GLOBALES*)
                                  
  let max_comm_size = 1024*1024
  let buffer = Bytes.create max_comm_size

  (*Processus*)
  let input = ref None (*Some file_descr*)
  let output = ref None (*Some file_descr*)

  (*Client
    Note : les processus sont identifiés pour chaque machine par un id.
    Au plus max_int processus peuvent être exécutés sur une même connexion.
    Les processus bindés ne comptent pas pour de nouveau processus *)
  let proc_inputs = Hashtbl.create 5 (*file_descr des inputs des process*)
  let proc_outputs = Hashtbl.create 5 (*file_descr des outputs des process*)
  let pipe_process_corres = Hashtbl.create 5 (*correspondance entre processus et pipes*)
  let need_pipe = Queue.create () (*Queue des processus en attente d'un nouvel id de channel*)



  (*Serveur*)
                
  let new_chan = let r=ref 0 in (fun () -> incr r; !r)
  let new_proc = let s=ref 0 in (fun () -> incr s; !s)
  let new_client = let t=ref 0 in (fun () -> incr t; !t)
                              
  let client_inputs = Hashtbl.create 5
  let client_outputs = Hashtbl.create 5
  let clients = Hashtbl.create 5 (*Ensemble des clients de type client_state*)
  let doco_counts = Hashtbl.create 5 (*contient des doco_count pour chaque process de type doco*)
  let pipes = Hashtbl.create 5 (*contient les file_descr des pipes*)
  let waiting_for_data = ref [] (*Liste des channels en attente de données.
                                  On pourrait faire un fork et un read bloquant, mais après
                                  il y aurai peut-être des choses bizarres lors du write dans le
                                  socket pour l'envoi*)
  let global_result = ref None (*résultat final de l'exécution
                                 C'est le résultat du processus d'id 0
                                 Si cette variable ne contient pas None, la routine serveur
                                 termine.*)

  (* FONCTIONS DE LECTURE ET D'ECRITURE *)
                  
  let read_comm fd =
    let n = Unix.read fd buffer 0 Marshal.header_size in
    if n=0 then
      0
    else
      let size = Marshal.data_size buffer 0 in
      let temp = Unix.read fd buffer Marshal.header_size size in
      if temp <> size then
        failwith "Erreur lors de la transmission de données : données incomplètes"
      else
        size + Marshal.header_size

  let write_comm comm fd =
    let b = Marshal.to_bytes comm [Closures] in
    if Bytes.length b > max_comm_size then failwith "trying to send data too big";
    let _ = Unix.write fd b 0 (Bytes.length b) in
    ()


  (* FONCTIONS CLIENT *)               
             
  (*Fonctions de lecture et d'écriture*)
                 
  let send_data comm =
    match (!output) with
    |Some chan -> write_comm comm chan
    |None -> failwith "No communications are enabled for this process"

  let send_bytes b length=
    match (!output) with
    |Some chan ->
      let _ = Unix.write chan b 0 length in
      ()
    |None -> failwith "No communications are enabled for this process"


  let get_data () =
    match (!input) with
    |Some chan ->
      let rec loop () = match read_comm chan with
        |0 -> loop ()
        |_ -> ()
      in loop ();
      let (d: 'a communication) = Marshal.from_bytes buffer 0 in d (*On assure le typage de d*)
    |None -> failwith "No communications are enabled for this process"


  (*Gestion de l'interface clients/processus*)

  let transmit_proc_comm proc_id proc_output =
    match read_comm proc_output with
    |0 -> ()
    |n -> let (d: 'a communication) = Marshal.from_bytes buffer 0 in
          begin
            match d with
            |DATA d -> ()
            |RESULT _ -> Hashtbl.remove proc_inputs proc_id;
                         Hashtbl.remove proc_outputs proc_id 
            |DATA_REQUEST port -> Hashtbl.add pipe_process_corres port proc_id
            |CHANNEL_QUERY -> Queue.push proc_id need_pipe
            |DOCO _ -> ()
            |_ -> failwith "a process cannot generate such communication"
          end;
          send_bytes buffer n (*Vérifier que write ne modifie pas le buffer*)


  (*Gestion de l'interface client/serveur*)

  let write_in_proc size proc =
    try      
      let proc_fd = Hashtbl.find proc_inputs proc in
      let _ = Unix.write proc_fd buffer 0 size in
      ()
    with Not_found -> failwith "Tentative de communication avec un processus inexistant"

  let apply_instruction size comm = match comm with
    |CHANNEL n ->
      begin
        try
          let proc = Queue.take need_pipe in
          write_in_proc size proc
        with Queue.Empty -> failwith "Attribution d'un channel non demandé"
      end
    |DATA d ->
      begin
        try
          let proc = Hashtbl.find pipe_process_corres d.target in
          write_in_proc size proc
        with Not_found -> failwith "La cible de la communication n'est pas sur ce client"
      end
    |EXECUTE p ->
      let temp_input, proc_input = Unix.pipe () in
      let proc_output, temp_output = Unix.pipe () in
      let proc_id = p.id in
      Hashtbl.add proc_outputs proc_id proc_output;
      Hashtbl.add proc_inputs proc_id proc_input;
      begin
        match Unix.fork () with
        |0 ->
          input := Some temp_input;
          output := Some temp_output;
          let v = p.proc () in
          send_data (RESULT {target = p.id; data = v});
          exit 0
        |_ -> ()
      end
    |DOCO_CONFIRM proc ->
      write_in_proc size proc
    |DC_CONFIRM -> exit 0
    |_ -> failwith "Une instruction n'ayant aucun sens a été reçue du serveur"

  let transmit_server_comm () =
    match (!input) with
    |None -> failwith "Connexion au serveur échouée, pas de socket paramétré"
    |Some chan ->
      let rec aux () =
        match read_comm chan with
        |0 -> ()
        |n -> let (d: 'a communication) = Marshal.from_bytes buffer 0 in
              apply_instruction n d;
              aux ()
      in aux ()
      

  (*Fonctions de connexion*)

  let rec client_routine socket =
    Hashtbl.iter transmit_proc_comm proc_outputs;
    transmit_server_comm ();
    client_routine socket
    

  let make_addr name port =
    let entry = Unix.gethostbyname name in
    Unix.ADDR_INET (entry.h_addr_list.(0),port)

  let run_client f addr =
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect socket addr;
    f socket

  let init serv port =
    run_client client_routine (make_addr serv port)


  (*FONCTIONS D'ÉCRITURE DE PROGRAMMES*)
    

  let new_channel () =
    send_data (CHANNEL_QUERY);
    let d = get_data () in
    match d with
    |CHANNEL c -> c,c
    |_ -> failwith "Wrong communication input, expected a channel"

    
  (* Attention : buffer des pipes de 65536 octets, il faut donc fork avant d'écrire 
     -> non, tout sera stocké sur le serveur *)

  let put ?flags:(flags = []) x c =
    let p = fun () -> send_data (DATA { target = c; data = x })
    in {proc = p; flags = flags; id = -1}

  let get ?flags:(flags = []) (c: 'a in_port) =
    let p =
      fun () -> let d = get_data () in
                match d with
                |DATA x ->
                  if x.target <> c then
                    failwith "Wrong target for communication"
                  else
                    x.data
                |_ -> failwith "Wrong communication input, expected some data"
    in {proc = p; flags = flags; id = -1}


  let return ?flags:(flags = []) x =
    let p= fun () -> x
    in {proc = p; flags = flags; id = -1}

  let bind p1 f =
    let p = fun () -> let v = p1.proc () in
                      let p2 = f v in
                      p2.proc ()
    in {proc = p; flags = p1.flags; id = p1.id}

  let send_doco l =
    assert false (*TODO : envoyer au serveur la liste de processus à répartir*)


  let doco l =
    {proc = (fun () -> send_doco l); flags = []; id = -1}
    



  (* FONCTIONS SERVEUR *)

  let rec accept_clients () =
    assert false (*C'est le plus compliqué*)

  let send_message client_id comm =
    match Hashtbl.find_opt client_inputs client_id with
    |None -> failwith "Envoi d'une donnée à un client inexistant"
    |Some fd -> write_comm comm fd

  let init_disconnection client_id =
    let state = Hashtbl.find clients client_id in
    state.connected <- false


  let find_minimal_client () =
    let f client state res =
      let (minload,minclient) = res in
      if state.connected && (minclient = -1 || state.load < minload) then
        (state.load, client)
      else
        res
    in snd (Hashtbl.fold f clients (-1,-1))

  let incr_load client_id = match Hashtbl.find_opt clients client_id with
    |None -> failwith "Tentative d'attribution de processus à un client inexistant"
    |Some state -> state.load <- state.load + 1

  let rec distribute (l: unit process list) target = match l with
    |[] -> ()
    |p::q -> let id = new_proc () in
             p.id <- id;
             let final_process = bind p (fun () -> return target) in
             let c = find_minimal_client () in
             if c= -1 then failwith "No client available for this process";
             incr_load c;
             send_message c (EXECUTE final_process)
             
             


  let handle_communication client_id size d = match d with
    |DC_QUERY -> init_disconnection client_id
    |CHANNEL_QUERY ->
      let c = new_chan () in
      send_message client_id (CHANNEL c)
    |DATA d ->
      begin
        match (Hashtbl.find_opt pipes d.target) with
        |Some p -> let _ = Unix.write p.input buffer 0 size in ()
        |None -> let temp_output, temp_input = Unix.pipe () in
                 Hashtbl.add pipes d.target { input = temp_input ; output = temp_output};
                 let _ = Unix.write temp_input buffer 0 size in ()
      end
    |DATA_REQUEST channel ->
      waiting_for_data := {pipe_id = channel; client_id = client_id}::(!waiting_for_data)
    |DOCO x -> let length = List.length x.content in
               Hashtbl.add doco_counts x.target {current = 0; total = length; client = client_id};
               distribute x.content x.target
    |RESULT x ->
      begin
        try
          let state = Hashtbl.find clients client_id in
          state.load <- state.load -1
        with Not_found -> failwith "Retour d'un client inexistant"
      end;
      if x.target = 0 then
        global_result := Some x.data
      else
        let parent = x.data in
        begin
          try
            let count = Hashtbl.find doco_counts parent in
            count.current <- count.current + 1;
            if count.current = count.total then
              begin
                send_message count.client (DOCO_CONFIRM parent);
                Hashtbl.remove doco_counts parent
              end
          with Not_found -> failwith "Présence d'un processus orphelin"
        end
    |_ -> failwith "Communication impossible vers le serveur reçue"
    

  let rec handle_client client_id client_output =
    match read_comm client_output with
    |0 -> ()
    |n -> let (d: 'a communication) = Marshal.from_bytes buffer 0 in
          handle_communication client_id n d;
          handle_client client_id client_output

  let handle_data_requests () =
    let rec aux l = match l with
      |[] -> []
      |x::q ->
        match (Hashtbl.find_opt pipes x.pipe_id) with
        |None -> x::(aux q)
        |Some p ->
          let n = Unix.read p.output buffer 0 Marshal.header_size in
          if n = 0 then
            x::(aux q)
          else
            let size = Marshal.data_size buffer 0 in
            let _ = Unix.read p.output buffer Marshal.header_size size in
            try
              let fd = Hashtbl.find client_inputs x.client_id in
              let _ = Unix.write fd buffer 0 (size + Marshal.header_size) in
              aux q
            with
              Not_found -> failwith "Tentative de communication avec un client inexistant"
             |_ -> failwith "Une erreur est survenue lors de l'envoi des données"
    in      
    waiting_for_data := aux (!waiting_for_data)

  let disconnect_clients () =
    let f client_id state l=
      if (not state.connected) && (state.load = 0) then
        client_id::l
      else
        l
    in
    let l_dc = Hashtbl.fold f clients [] in
    let g client_id =  Hashtbl.remove client_inputs client_id;
                       Hashtbl.remove client_outputs client_id;
                       Hashtbl.remove clients client_id
    in
    List.iter g l_dc  
          

  let rec server_routine () =
    disconnect_clients ();
    accept_clients ();
    Hashtbl.iter handle_client client_outputs;
    handle_data_requests ();
    match (!global_result) with
    |None -> server_routine ()
    |Some x -> x
    


  let run p = assert false (* TODO : init le serveur, les relations avec les clients, 
                              et lancer le process*)   


    
    


            
end
