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
  val bind: ?flags: flag list -> 'a process -> ('a -> 'b process) -> 'b process

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
                      id : int;
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

  type 'a communication = DC_QUERY
                        | DC_CONFIRM (*Quand le client n'a plus de processus actifs*)
                        | CHANNEL_QUERY
                        | CHANNEL of int
                        | DATA of 'a data
                        | DATA_REQUEST of 'a port
                        | DOCO of unit process list
                        | DOCO_CONFIRM of int (*Message du serveur à un processus signalant que les processus parallèles sont terminés*)
                        | EXECUTE of 'a process
                        | RESULT of 'a data (*ici, le target est le numéro du processus envoyant le résultat*)
                           
                                

  (*VARIABLES GLOBALES*)
                                
  let input = ref None (*Some file_descr*)
  let output = ref None (*Some file_descr*)

  let max_comm_size = 1024*1024
  let buffer = Bytes.create max_comm_size

  (*Note : les processus sont identifiés pour chaque machine par un id. Au plus max_int processus peuvent être exécutés sur une même connexion.
    Les processus bindés ne comptent pas pour de nouveau processus *)
  let proc_inputs = Hashtbl.create 5 (*file_descr des inputs des process*)
  let proc_outputs = Hashtbl.create 5 (*file_descr des outputs des process*)
  let pipe_process_corres = Hashtbl.create 5 (*correspondance entre processus et pipes*)
  let need_pipe = Queue.create () (*Queue des processus en attente d'un nouvel id de channel*)


  let new_id = let r=ref 0 in (fun () -> incr r; !r)
             


  (* FONCTIONS CLIENT *)               
             
  (*Fonctions de lecture et d'écriture*)
                 
  let send_data comm =
    match (!output) with
    |Some chan ->
      let b = Marshal.to_bytes comm [Closures] in
      if Bytes.length b > max_comm_size then failwith "trying to send data too big";
      let _ = Unix.write chan b 0 (Bytes.length b) in
      ()
    |None -> failwith "No communications are enabled for this process"

  let send_bytes b length=
    match (!output) with
    |Some chan ->
      let _ = Unix.write chan b 0 length in
      ()
    |None -> failwith "No communications are enabled for this process"

  let read_all fd =
    let rec aux ofs =
      if ofs <> max_comm_size then
        let test = Unix.read fd buffer ofs 1 in
        if test <> 0 then aux (ofs+1)
        else ofs
      else ofs
    in aux 0

  let get_data () =
    match (!input) with
    |Some chan ->
      let rec loop () = match read_all chan with
        |0 -> loop ()
        |_ -> ()
      in loop ();
      let (d: 'a communication) = Marshal.from_bytes buffer 0 in d (*On assure le typage de d*)                 
    |None -> failwith "No communications are enabled for this process"


  (*Gestion de l'interface clients/processus*)

  let transmit_proc_comm proc_id proc_output =
    match read_all proc_output with
    |0 -> ()
    |n -> send_bytes buffer n; (*Vérifier que write ne modifie pas le buffer*)
          let (d: 'a communication) = Marshal.from_bytes buffer 0 in
          match d with
          |DATA d -> ()
          |RESULT d -> Hashtbl.remove proc_inputs proc_id;
                       Hashtbl.remove proc_outputs proc_id 
          |DATA_REQUEST port -> Hashtbl.add pipe_process_corres port proc_id
          |CHANNEL_QUERY -> Queue.push proc_id need_pipe
          |DOCO l -> ()
          |_ -> failwith "a process cannot generate such communication"


  (*Gestion de l'interface client/serveur*)

  let write_in_proc ofs size proc =
    try      
      let proc_fd = Hashtbl.find proc_inputs proc in
      let _ = Unix.write proc_fd buffer ofs size in
      ()
    with Not_found -> failwith "Tentative de communication avec un processus inexistant"

  let apply_instruction ofs size comm = match comm with
    |CHANNEL n ->
      begin
        try
          let proc = Queue.take need_pipe in
          write_in_proc ofs size proc
        with Queue.Empty -> failwith "Attribution d'un channel non demandé"
      end
    |DATA d ->
      begin
        try
          let proc = Hashtbl.find pipe_process_corres d.target in
          write_in_proc ofs size proc
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
      write_in_proc ofs size proc
    |_ -> failwith "Une instruction n'ayant aucun sens a été reçue du serveur"

  let transmit_server_comm () =
    match (!input) with
    |None -> failwith "Connexion au serveur échouée, pas de socket paramétré"
    |Some chan ->
      begin
        match read_all chan with
        |0 -> ()
        |n -> let rec aux ofs =
                try
                  let (d: 'a communication) = Marshal.from_bytes buffer ofs in
                  let size = Marshal.total_size buffer ofs in
                  apply_instruction ofs size d;
                  aux (ofs + size)
                with End_of_file -> ()
                   | _ -> failwith "Une erreur est survenue lors de la lecture des données envoyées par le serveur"
              in aux 0
      end



  (*Fonctions de connexion*)

  let rec routine socket =
    Hashtbl.iter transmit_proc_comm proc_outputs;
    transmit_server_comm ();
    routine socket
    

  let make_addr name port =
    let entry = Unix.gethostbyname name in
    Unix.ADDR_INET (entry.h_addr_list.(0),port)

  let run_client f addr =
    let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    Unix.connect socket addr;
    f socket

  let init serv port =
    run_client routine (make_addr serv port)

    


    

  (*FONCTIONS D'ÉCRITURE DE PROGRAMMES*)
    

  let new_channel () =
    assert false (*TODO : Demander au serveur un id de pipe frais*)

    
  (* Attention : buffer des pipes de 65536 octets, il faut donc fork avant d'écrire -> non, tout sera stocké sur le serveur *)

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
                |_ -> failwith "Wrong communication input"
    in {proc = p; flags = flags; id = -1}


  let return ?flags:(flags = []) x =
    let p= fun () -> x
    in {proc = p; flags = flags; id = -1}

  let bind ?flags:(flags = []) p1 f =
    let p = fun () -> let v = p1.proc () in
                      let p2 = f v in
                      p2.proc ()
    in {proc = p; flags = flags; id = p1.id}

  let distribute l =
    assert false (*TODO : envoyer au serveur la liste de processus à répartir*)


  let doco l =
    {proc = (fun () -> distribute l); flags = []; id = -1}
    



  (* FONCTIONS SERVEUR *)

  (*TODO : faire la fonction serveur qui écoute les requetes des clients,
    et celle qui organise la répartition des ressources*)

  let run p = assert false (* TODO : init le serveur, les relations avec les clients, 
                              et lancer le process*)
            
end
