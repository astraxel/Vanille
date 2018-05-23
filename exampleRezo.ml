open Arg

module Example (K : Rezo.S) = struct
  module K = K
  module Lib = Rezo.Lib(K)
  open Lib

  let integers (qo : int K.out_port) : unit K.process =
    let rec loop n =
      (K.put n qo) >>=  (fun () -> loop (n + 1))
    in
    loop 2

  let output (qi : int K.in_port) : unit K.process =
    let rec loop () =
      (K.get qi) >>= (fun v -> Format.printf "%d@." v; loop ())
    in
    loop ()

  let main : unit K.process =
    K.new_channel () >>=
    (fun (q_in, q_out) -> K.doco [ integers q_out ; output q_in ; ])

end

module E = Example(Rezo.Th)

let client = ref false
let port = ref 0
let server = ref ""

let () =
  let usage = "usage: ./exampleRezo [options]" in

  let spec =
    [
      "-c", Arg.Set client , "Enable for clients, disable for server";
      "--server_adress", Arg.Set_string server, "Adress of server. Used only for clients";
      "-p", Arg.Set_int port, "Choose a different port for communication. Default is 1042";
    ]
  in
  
  Arg.parse spec print_endline usage;
  print_int 5;

  if (!client) then
    (print_string "Demarrage du client\n";
    match (!port) with
    |0 -> E.K.init_client (!server)
    |n -> E.K.init_client ~port:n (!server)
    )
  else
    let () = print_string "Demarrage du serveur\n" in
    match (!port) with
    |0 -> E.K.run E.main
    |n -> E.K.run ~port:n E.main
    
