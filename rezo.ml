module type S = sig
  type flag
  type 'a process
  type 'a in_port
  type 'a out_port

  val new_channel: unit -> ('a in_port * 'a out_port) process
  val put: ?flags: flag list -> 'a -> 'a out_port -> unit process
  val get: ?flags: flag list -> 'a in_port -> 'a process

  val doco: unit process list -> unit process

  val return: ?flags: flag list -> 'a -> 'a process
  val bind: 'a process -> ('a -> 'b process) -> 'b process

  val run: 'a process -> 'a
end

module type Lib  =
  functor (K:S) ->
  sig
    val ( >>= ) : 'a K.process -> ('a -> 'b K.process) -> 'b K.process
      
    val delay : 'a -> ('a -> 'b) -> 'b K.process

    val par_map : ('a -> 'b) -> 'a list -> 'b list
  end
  
module Lib (K : S) = struct

  let ( >>= ) x f = K.bind x f

  let delay f x =
    K.bind (K.return ()) (fun () -> K.return (f x))


  let par_map f l =
    let rec build_channels l accin accout = match l with
      |[] -> K.return (accin,accout)
      |x::q -> K.bind (K.new_channel ()) (fun (qi,qo) -> build_channels q (qi::accin) (qo::accout))
    in
    let rec build_workers l accout = match l with
      |[] -> []
      |x::q -> let qo::r=accout in
               ((delay f x) >>= (fun v -> K.put v qo))::(build_workers q r)
    in
    let rec collect accin = match accin with
      |[] -> K.return []
      |qi::r -> collect r >>= (fun l -> ((K.get qi) >>= (fun v -> K.return (v::l))))
    in
    let first_part l qo =
      build_channels l [] [] >>= (fun (accin,accout) -> K.doco ((K.put (collect accin) qo)::(build_workers l accout)))
    in
    K.run
      (K.new_channel () >>= (fun (qi,qo) -> (first_part l qo) >>= (fun _ -> K.get qi)))
 

end
              
module Th: S = struct
  
  type flag = MACHINE of int
            (* Les processus avec le flag MACHINE x identiques seront exécutés sur la même machine.
               Note : Dans le cas d'un bind, le flag doit être précisé sur le premier processus.
               Cela permet de maintenir des processus communiquant entre eux sur la même machine,
               et donc de réduire les communications réseau. -> non implémenté
             *)
            
  type 'a process = { proc : (Unix.file_descr option -> Unix.file_descr option -> unit -> 'a);
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

                       On doit passer les input/output en argument sinon Marshall fait de la merde
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
  let parents = Hashtbl.create 5 (*contient les parents des processus distribués par doco*)
  let pipes = Hashtbl.create 5 (*contient les pipes entre processus, hébergées sur serveur*)
  let waiting_for_data = ref [] (*Liste des channels en attente de données.
                                  On pourrait faire un fork et un read bloquant, mais après
                                  il y aurai peut-être des choses bizarres lors du write dans le
                                  socket pour l'envoi*)
  let port = ref 1042 (*Port de connection du serveur*)

  (* FONCTIONS DE LECTURE ET D'ECRITURE *)
                  
  let read_comm fd =
    try
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
    with
      Unix.Unix_error (Unix.EAGAIN,_,_) | Unix.Unix_error (Unix.EWOULDBLOCK,_,_) -> 0

  let write_comm comm fd =
    let b = Marshal.to_bytes comm [Marshal.Closures] in
    if Bytes.length b > max_comm_size then failwith "trying to send data too big";    
    let _ = Unix.write fd b 0 (Bytes.length b) in
    ()


  (* FONCTIONS CLIENT *)               
             
  (*Fonctions de lecture et d'écriture*)
                 
  let send_data ?o:(o= !output) comm =
    match o with
    |Some chan -> write_comm comm chan
    |None -> failwith "No communications are enabled for this process"

  let send_bytes ?o:(o= !output) b length=
    match o with
    |Some chan ->
      let _ = Unix.write chan b 0 length in
      ()
    |None -> failwith "No communications are enabled for this process"


  let get_data ?i:(i= !input) () = (*En théorie, les pipes concernées sont blocantes, donc inutile*)
    match i with
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
            |DATA d -> send_bytes buffer n
            |RESULT _ -> Hashtbl.remove proc_inputs proc_id;
                         Hashtbl.remove proc_outputs proc_id;
                         send_bytes buffer n
            |DATA_REQUEST port -> Hashtbl.add pipe_process_corres port proc_id;
                                  send_bytes buffer n
            |CHANNEL_QUERY -> Queue.push proc_id need_pipe;
                              send_bytes buffer n
            |DOCO d ->
              let l = d.content in
              let real_d = {content = l; target = proc_id} in
              send_data real_d
            |_ -> failwith "a process cannot generate such communication"
          end
          


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
      Unix.set_nonblock proc_output;
      let proc_id = p.id in
      Hashtbl.add proc_outputs proc_id proc_output;
      Hashtbl.add proc_inputs proc_id proc_input;
      begin
        match Unix.fork () with
        |0 ->
          input := Some temp_input;
          output := Some temp_output;
          let v = p.proc (Some temp_input) (Some temp_output) () in
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

  let transmit_IO_comm pipout =
    try
      match (Unix.read pipout buffer 0 1) with
      |0 -> ()
      |_ -> send_data DC_QUERY
    with
    |Unix.Unix_error (Unix.EAGAIN,_,_) | Unix.Unix_error (Unix.EWOULDBLOCK,_,_) -> ()

  let rec client_routine pipout socket =
    Hashtbl.iter transmit_proc_comm proc_outputs;
    transmit_server_comm ();
    transmit_IO_comm pipout;
    client_routine pipout socket
    

  let make_addr name port =
    let entry = Unix.gethostbyname name in
    Unix.ADDR_INET (entry.h_addr_list.(0),port)

  let run_client  f addr =
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect socket addr;
    f socket

  let rec run_IO pipin =
    print_string "Tapez \q pour initier la déconnexion du client";
    if (read_line ()) = "\q" then
      let _ = Unix.write_substring pipin "OK" 0 2 in ();
    else
      run_IO pipin

  let init serv port =
    let pipout,pipin = Unix.pipe () in
    Unix.set_nonblock pipout;
    match Unix.fork () with
    |0 -> run_client (client_routine pipout) (make_addr serv port)
    |n -> run_IO pipin


  (*FONCTIONS D'ÉCRITURE DE PROGRAMMES*)

  let new_channel_process i o () =
    send_data ~o:o (CHANNEL_QUERY);
    let d = get_data ~i:i () in
    match d with
    |CHANNEL c -> c,c
    |_ -> failwith "Erreur lors de la création d'un nouveau channel"      
    

  let new_channel () =
    {proc = new_channel_process;
     flags = [];
     id = -1
    }
  (*On a changé la signature de new_channel à cause du problème de Closures sur Marshall*)
    

    
  (* Attention : buffer des pipes de 65536 octets, il faut donc fork avant d'écrire 
     -> non, tout sera stocké sur le serveur *)

  let put_process x c i o () =
    send_data ~o:o (DATA { target = c; data = x })
    
  let put ?flags:(flags = []) x c =
    {proc = put_process x c; flags = flags; id = -1}

  let get_process c i o () =
    let d = get_data ~i:i () in
    match d with
    |DATA x ->
      if x.target <> c then
        failwith "La cible de la communication est incorrecte"
      else
        x.data
    |_ -> failwith "Erreur de format : des données étaient attendues"

  let get ?flags:(flags = []) (c: 'a in_port) =
    {proc = get_process c; flags = flags; id = -1}


  let return_process x i o () =
    x

  let return ?flags:(flags = []) x =
    {proc = return_process x; flags = flags; id = -1}

  let bind_process p1 f i o () =
    let v = p1.proc i o () in
    let p2 = f v in
    p2.proc i o ()

  let bind p1 f =
    {proc = bind_process p1 f; flags = p1.flags; id = p1.id}

  let send_doco l i o ()=
    send_data ~o:o (DOCO {content = l; target = -1})


  let doco l =
    {proc = send_doco l; flags = []; id = -1}
    



  (* FONCTIONS SERVEUR *)

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
             Hashtbl.add parents id target;
             let c = find_minimal_client () in
             if c= -1 then failwith "No client available for this process";
             incr_load c;
             send_message c (EXECUTE p)
             

  let handle_communication client_id size d = match d with
    |DC_QUERY -> init_disconnection client_id; None
    |CHANNEL_QUERY ->
      let c = new_chan () in
      send_message client_id (CHANNEL c);
      None
    |DATA d ->
      begin
        match (Hashtbl.find_opt pipes d.target) with
        |Some q -> Queue.add (Bytes.sub buffer 0 size) q
        |None ->
          let q = Queue.create () in
          Queue.add (Bytes.sub buffer 0 size) q;
          Hashtbl.add pipes d.target q
      end;
      None
    |DATA_REQUEST channel ->
      waiting_for_data := {pipe_id = channel; client_id = client_id}::(!waiting_for_data);
      None
    |DOCO x -> let length = List.length x.content in
               Hashtbl.add doco_counts x.target {current = 0; total = length;  client = client_id};
               distribute x.content x.target;
               None
    |RESULT x ->
      begin
        try
          let state = Hashtbl.find clients client_id in
          state.load <- state.load -1
        with Not_found -> failwith "Retour d'un client inexistant"
      end;
      if x.target = 0 then
        Some x.data
      else
        let parent = Hashtbl.find parents x.target in
        Hashtbl.remove parents x.target;
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
        end;
        None
    |_ -> failwith "Communication impossible vers le serveur reçue"
    

  let rec handle_client client_id client_output =
    match read_comm client_output with
    |0 -> None
    |n -> let (d: 'a communication) = Marshal.from_bytes buffer 0 in
          let res = handle_communication client_id n d in
          match handle_client client_id client_output with
          |None -> res
          |x -> x

  let handle_data_requests () =
    let rec aux l = match l with
      |[] -> []
      |x::q ->
        match (Hashtbl.find_opt pipes x.pipe_id) with
        |None -> x::(aux q)
        |Some c ->
          if Queue.is_empty c then
            x::(aux q)
          else
            try
              let message = Queue.take c in
              let fd = Hashtbl.find client_inputs x.client_id in
              let _ = Unix.write fd message 0 (Bytes.length message) in
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


  let rec accept_clients socket () =
    try      
      let fd,_ = Unix.accept socket in
      let client_id = new_client () in
      Hashtbl.add client_inputs client_id fd;
      Hashtbl.add client_outputs client_id fd;
      Hashtbl.add clients client_id {load=0; connected=true};
      accept_clients socket ()
    with
    |Unix.Unix_error (Unix.EAGAIN,_,_) |Unix.Unix_error (Unix.EWOULDBLOCK,_,_) -> ()
    |_ -> failwith "Erreur lors de la connexion d'un client"

  let rec start first_process = match first_process with
    |None -> None
    |Some p  -> let c = find_minimal_client () in
                if c= -1 then Some p
                else
                  begin
                    incr_load c;
                    send_message c (EXECUTE p);
                    None
                  end


        

  let rec server_routine socket first_process () =  
    disconnect_clients ();
    accept_clients socket ();
    let fp = start first_process in
    let f  k fd res = match handle_client k fd with
      |None -> res
      |x -> x
    in
    let result = Hashtbl.fold f client_outputs None in
    handle_data_requests ();
    match result with
    |None -> server_routine socket fp ()
    |Some x -> x
    


  let run p =
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM (!port) in
    Unix.set_nonblock socket;
    Unix.bind socket (Unix.ADDR_INET (Unix.inet_addr_any,!port));
    Unix.listen socket 10;
    p.id <- 0;
    server_routine socket (Some p) ()
    
    
    


            
end
