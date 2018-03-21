module type S = sig
  type 'a process
  type 'a in_port
  type 'a out_port

  val new_channel: unit -> 'a in_port * 'a out_port
  val put: 'a -> 'a out_port -> unit process
  val get: 'a in_port -> 'a process

  val doco: unit process list -> unit process

  val return: 'a -> 'a process
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
  type 'a process =
    |End of 'a
    |Compute of (unit -> 'a process)

  type 'a channel = {q : 'a Queue.t ; m : Mutex.t;}
  type 'a in_port = 'a channel 
  type 'a out_port = 'a channel

  let new_channel () =
    let q = {q = Queue.create (); m = Mutex.create (); } in
    q,q

  let put v c =
    Compute ( fun () -> 
              Mutex.lock c.m;
              Queue.push v c.q;
              Mutex.unlock c.m;
              End () )

  let rec get c =
    Compute ( fun () ->
              try
                Mutex.lock c.m;
                let v = Queue.pop c.q in
                Mutex.unlock c.m;
                End v
              with
                Queue.Empty -> Mutex.unlock c.m;
                               get c
            )

  let rec doco l =
    let rec finish_process = function
      |[]          -> []
      |(End ())::q -> finish_process q
      |p::q        -> p :: (finish_process q)
    in
    let compute_step = function
      |End x -> failwith "process ended"
      |Compute p -> p ()
    in
    match finish_process l with
    |[] -> End ()
    |l1 -> Compute ( fun () ->
                     doco (List.map compute_step l1)
                   )

  let return v = End v

  let rec bind e1 e2 = match e1 with
    |End v -> e2 v
    |Compute p -> Compute ( fun () ->
                            bind (p ()) e2
                          )

  (*let step = function
    |End v -> v
    |Compute p -> p ()*)

  let rec run = function
    |End v -> v
    |Compute p -> run (p ())
            
end
