module type S = sig
  type flag
  type 'a process
  type 'a in_port
  type 'a out_port

  val new_channel: unit -> 'a in_port * 'a out_port
  val put: 'a -> 'a out_port -> ?flags: flag list -> unit process
  val get: 'a in_port -> ?flags: flag list -> 'a process

  val doco: unit process list -> unit process

  val return: 'a -> ?flags: flag list -> 'a process
  val bind: 'a process -> ('a -> 'b process) -> ?flags: flag list -> 'b process

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
                    }
                    (* Chaque process aura un unique moyen pour communiquer avec le processus
                       principal de la machine qui s'occupe de la communication avec le serveur.
                       Aucun input/output n'est crée si le process n'est pas encore affecté
                       ou si le flag NO_COMMUNICATION est spécifié.
                       Cf variables globales input et output.
                     *)


                  

  (* TYPES DE COMMUNICATION *)
  
  type 'a data = { target : int ;
                   data : 'a;
                 }

  type 'a communication = DC_QUERY
                        | DC_CONFIRM
                        | CHANNEL_QUERY
                        | CHANNEL of int
                        | DATA of 'a data
                        | DOCO of unit process list                               
                           
                                

  (*VARIABLES GLOBALES*)
                                
  let input = ref None (*Some in_channel*)
  let output = ref None (*Some out_channel*)



             

  (*FONCTIONS D'ÉCRITURE DE PROGRAMMES*)
             
                  
  type 'a in_port = int
  type 'a out_port = int
  (*Le client assurera la communication avec le serveur, une pipe étant identifiée uniquement par
    un numéro.
    Outre le respect de la signature, le 'a indiquera les types au module Marshall.
    Ça marche au typage ?*)
                   
  let new_channel () =
    assert false (*TODO : Demander au serveur un id de pipe frais*)

  
  (* Attention : buffer des pipes de 65536 octets, il faut donc fork avant d'écrire *)

  let put x c ?flags:(flags = []) =
    let p=
      fun () -> match (!output) with
                |Some chan -> Marshal.to_channel chan (DATA { target = c; data = x }) [Closures]
                |None -> failwith "No communications are enabled for this process"
    in {proc = p; flags = flags}

  let get (c: 'a in_port) ?flags:(flags = []) =
    let p=
      fun () -> match (!input) with
                |Some chan ->
                  begin
                    let (d: 'a communication) = Marshal.from_channel chan in
                    match d with
                    |DATA x ->
                      if x.target <> c then
                        failwith "Wrong target for communication"
                                 (*TODO : si plusieurs pipes arrivent sur le même processus*)
                      else
                        x.data
                    |_ -> failwith "Wrong communication input"
                  end
                |None -> failwith "No communications are enabled for this process"
    in {proc = p; flags = flags}


  let return x ?flags:(flags = []) =
    let p= fun () -> x
    in {proc = p; flags = flags}

  let bind p1 f ?flags:(flags = []) =
    let p = fun () -> let v = p1.proc () in
                      let p2 = f v in
                      p2.proc ()
    in {proc = p; flags = flags}

  let distribute l =
    assert false (*TODO : envoyer au serveur la liste de processus à répartir*)


  let doco l =
    {proc = (fun () -> distribute l); flags = []}
      
    

  (* FONCTIONS CLIENT *)               
                  
  type id_client = int
                 
  (*TODO : faire la fonction du client qui retransmets toutes les communications
    et écoute en permanance sur les différents channels *)





  (* FONCTIONS SERVEUR *)

  (*TODO : faire la fonction serveur qui écoute les requetes des clients,
    et celle qui organise la répartition des ressources*)

  let run p = assert false (* TODO : init le serveur, les relations avec les clients, 
                              et lancer le process*)
                  
end
