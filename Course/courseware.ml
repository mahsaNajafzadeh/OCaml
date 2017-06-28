let unimpl () = failwith "Unimpl"
let print x = ()


module U = Unix

 type id

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

type student = {s_id:id}
type course={c_id: id; mutable c_capacity: int }
type enroll={e_s_id: id; e_c_id: id}

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
			SQL.delete [Student] 
			 (fun st -> st.s_id=s_id ) 
			
(*
 * adding a new course. 
 *)
let add_course_txn	c_id  c_capacity =
	 let co= {c_id=c_id;  c_capacity=c_capacity } in 
		SQL.insert  Course co

(*
 * removing  a course. 
 *)

let remove_course_txn c_id =
			SQL.delete [Course]  
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
		let en= {e_s_id=st.s_id; e_c_id=co.c_id } in 
		  SQL.insert Enroll en
			
(*
 * removing  a course enrolmnet. 
 *)		
		
let disenroll_txn s_id c_id = 
			let co= SQL.select1 [Course] 
	   (fun co -> co.c_id= c_id) in
			let _=SQL.update  Course
		  (fun c -> c.c_capacity <- c.c_capacity +1 )
			(fun c -> c.c_id = c_id) in 
	  			SQL.delete [Enroll]  
			 (fun en -> en.e_c_id=c_id &&  en.e_s_id=s_id ) 
			
(*
 * Invariants
 *)
(*let inv1 () = 
   let css= SQL.select [Course]  in 
	S.for_all ( fun c -> 
		c.c_capacity >=0 ) css
	 
	 
	

let inv2()=
 let ens=SQL.select [Enroll] in 
S.for_all ( fun en -> 
	   	let c=SQL.select1 [Course] 
		      	 (fun c -> c.c_id= en.e_c_id ) in 
		 	 let s=SQL.select1 [Student] 
		      	 (fun s -> s.s_id=en.e_s_id ) in 
          match (c, s) with 
		| (Some _, Some _) -> true
               | _ -> false 
		
) ens
*)					 
					
					
					
						
						
						
						
						
		
		
		
		
		 
			
		
		
	
	
	 
	
	
	
	
	
	
			 
	
						
						
			
			
			
			 
			
			
			
			
			
			
			
			
		   
			
	
		
		
  

