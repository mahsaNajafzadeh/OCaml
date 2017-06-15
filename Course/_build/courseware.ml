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
 *)

let deregeister_txn s_id =
			SQL.delete Student  
			 (fun st -> st.s_id=s_id ) 
			
(*
 * adding a new course. 
 *)
let add_course_txn	c_id  c_capacity =
	 let co= {c_id=cid;  c_capacity=c_capacity } in 
		SQL.insert  Course co

(*
 * removing  a course. 
 *)

let remove_course_txn s_id =
			SQL.delete Course  
			 (fun co -> co.c_id=c_id ) 

(*
 * adding a new  course enrolmnet. 
 *)			

let  enroll_txn s_id c_id =
	let st=SQL.select1 [Student] 
	   (fun st -> st.s_id= s_id) in 
		let co= SQL.select1 [Course] 
	   (fun co -> co.c_id= c_id) in
		let _=SQL.update  Course
		  (fun c -> c.c_capacity <- c.c_capacity -1 )
			(fun c -> c.c_id = c_id) in 
		let new_enroll= {s_id=st.s_id; c_id=co.c_id } in 
		  SQl.insert Enroll new_enroll
			
(*
 * removing  a course enrolmnet. 
 *)		
		
let disenroll_txn s_id c_id = 
			let co= SQL.select1 [Course] 
	   (fun co -> co.c_id= c_id) in
			let _=SQL.update  Course
		  (fun c -> c.c_capacity <- c.c_capacity +1 )
			(fun c -> c.c_id = c_id) in 
	  			SQL.delete Enroll  
			 (fun en -> en.c_id=c_id &&  en.s_id=s_id ) 
			
	Printf.printf "Result: %s\n" ;;
	
			 
	
						
						
			
			
			
			 
			
			
			
			
			
			
			
			
		   
			
	
		
		
  

