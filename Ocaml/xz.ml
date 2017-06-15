let unimpl () = failwith "Unimpl"
let print x = ()


module U = Unix
 
 type table_name = 
   |Student
	| Course
	| Enroll
 
module SQL = struct
  let select = unimpl ()
  let select1 = unimpl ()
  let insert = unimpl ()
  let delete = unimpl ()
  let update = unimpl ()
end


module S = struct
  include List
  let size = length
  let foreach = iter
  let max f l = unimpl ()
  let min f l = unimpl ()
  let sum f l = unimpl ()
end

type student = {s_id: id}
type course={c_id: id; mutable c_capacity: int }
type enroll={s_id: id; c_id: id}

(*
 * New student regsitertion. 
 *)

let new_register_txn s_id = 
	let st= {s_id=s_id} in 
		SQL.insert Student st
		
(*
 * student deregsitertion. 
 *)==

let deregeister_txn s_id =
			let _=SQL.delete Student  
			 (fun st -> st.s_id=s_id ) in
			
			
			 
			
			
			
			
			
			
			
			
		   
			
	
		
		
  

