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
  type 'a process = (unit -> 'a)

  type 'a in_port = in_channel
  type 'a out_port = out_channel

  let new_channel () =
    let in_port, out_port = Unix.pipe () in
    Unix.in_channel_of_descr in_port, Unix.out_channel_of_descr out_port

  let put x c () =
    Marshal.to_channel c x [Closures]
            
  let get c () =
    Marshal.from_channel c
    
  let return x = fun () -> x
                         
  let run p = p ()

  let bind p1 p2 () =
    let x = run p1 in
    p2 x ()

  let doco l () =
    let rec aux l = match l with
      |[] -> exit 0
      |p::q ->
        begin
          match Unix.fork () with
          |0 -> aux q
          |n -> run p; let _ = Unix.wait () in exit 0
        end
    in
    begin
      match Unix.fork () with
      |0 -> aux l
      |n -> let _ = Unix.wait () in ()
    end

end
