let unimpl () = failwith "Unimpl"
let print x = ()


module U = Unix

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

 type id

 type table_name = 
	| Item
	| Customer
	| Order
	| Orderline
	| Shoppingcart
	| Cc_xacts
	| Address
	| Author
	| Shoppingcart_list
	| Result (*used for keeping intermediate reult *)
	
	type item = {i_id:id; mutable i_stock:int; mutable i_cost: int;  i_a_id: id; mutable i_subject: string;
	mutable  i_pub_date: Unix.tm option}
	type author= {a_id: id; mutable  a_name: string}
	type customer= {c_id: id;  c_addr_id: id }
	type  order={o_id: id;  o_c_id: id ; o_total: int; o_ship_addr_id: id}
	type orderline={ol_id: id; ol_o_id: id;  ol_i_id: id ; mutable ol_qty: int}
	type shoppingcart={sc_id: id; sc_c_id: customer; mutable sc_date: Unix.tm option;   
	mutable sc_total: int ;  sc_scl_id: id}
	type shoppingcart_list={scl_id: id; mutable scl_i_id: id ;  mutable scl_qty: int ;
	mutable scl_cost: int}
	type address= {addr_id: id; addr_co_id: id }
	type cc_xacts={cx_o_id: id; mutable cx_type: string;  mutable cx_num: int ;mutable cx_name: string; 
	cx_expiry:  Unix.tm option ; cx_xact_date:  Unix.tm option ; mutable cx_xact_amt: int; 
	cx_co_id: id }
	type result={r_i_id: id; mutable r_i_qty: int}

	(*
 *Customer register transaction
 *)		
	
  let cutomer_regsiter_txn c_id c_addr_id=
			let  c= {c_id; c_addr_id} in 
			SQL.insert Customer  c
			
(*
 *Shopping cart transaction
 *)	
			
	let		shopping_cart_interaction_txn  i_id     shopping_id c_id sc_scl_id = 
		let lis=		SQL.select1 [Shoppingcart ; Shoppingcart_list ]  
			 (fun s ->  s.sc_id=shopping_id )
			 (fun scl -> scl.scl_i_id=i_id )
		   (fun s -> fun scl -> s.sc_id=scl.scl_id )
			 in 
			let exists = match (lis) with
    						| None -> false
								| Some _ -> true in
			let item_id= i_id in  
		let _=		if (exists==true) then  
				begin
				 SQL.update  Shoppingcart_list 
				 (fun scl -> scl.scl_qty <- scl.scl_qty + 1) 
				 (fun scl -> scl.scl_i_id=i_id) 
				 (fun scl -> scl.scl_id= lis.sc_id) 
				 end
				else
			begin 
			let  	dts = Some (U.gmtime @@ U.time ())  in 		
				let	item=	SQL.select1 [Item]
			 (fun item -> item.i_id= item_id ) in  
			 let s=		SQL.select1 [Shoppingcart]  
			 (fun s ->  s.sc_id=shopping_id ) in 
						let s_exists = match (s) with
    						| None -> false
								| Some _ -> true in 
	    		if (s_exists==true) then  
					begin 
						let scl= {scl_id= s.scl_id ;
					scl_i_id=item.i_id;   scl_qty=1;  scl_cost=item.i_cost }	in 
					SQL.insert Shoppingcart_list  scl 
					end 
					else 
						begin 
			      let sc= {sc_id=shopping_id; sc_c_id= c_id; sc_date=dts ;sc_total= 0; sc_scl_id
					 }	in 
					let _=SQL.insert Shoppingcart  sc in 
										let scl= {scl_id= sc_scl_id ;scl_i_id=item.i_id;   scl_qty=1;  scl_cost=item.i_cost }	in 
					   SQL.insert Shoppingcart_list  scl 
					  end
						
					end 	
				in ()
	
(*
 *Buy request transaction
 *)	
							
		let buy_request_txn sc_id c_id =
			let items=		SQL.select [Shoppingcart ;Shoppingcart_list ]
			 (fun s -> s.sc_id=sc_id && s.sc_c_id=c_id ) 
			 (fun s -> fun scl -> s.sc_scl_id= scl.scl_id) 
			  in 
			  let totalcost=S.sum (fun it -> it.scl_cost*it.scl_qty   ) items
					 in
				  let dts = Some (U.gmtime @@ U.time ()) in 
				 SQL.update  Shoppingcart 
				 (fun s -> s.sc_total<- totalcost ) 
				 ( fun s -> s.sc_date <- dts ) 
				 (fun s -> s.sc_id=sc_id && s.sc_c_id=c_id )
	
	(*
 *Buy confirm transaction
 *)	
			
	let buy_confrim_txn o_c_id co_id  c_id sc_id o_id list (orderline_ids: id list) 
	cc_number cc_name cc_type cc_expiry=	
			let c=SQL.select1 [Customer]
			(fun c -> c.c_id= c_id) in 
			let addr=c.c_addr_id in 
		   let sc=SQL.select1 [Shoppingcart]
		 	(fun sc -> sc.sc_id= sc_id  ) in
			let total= sc.sc_total in 
		let o={o_id; o_c_id; o_total=total ; o_ship_addr_id=addr   } in 
		 let _= SQL.insert  Order o in 
		let items=SQL.select [Shoppingcart_list]
		 	(fun items -> items.scl_id= sc.sc_scl_id  ) in
			let i=0 in 
		let _=	S.foreach( fun item -> 
				let ol={ol_id= List.nth  orderline_ids i ;ol_o_id= o_id ;  ol_i_id= item.scl_i_id ;
				 ol_qty= item.scl_qty  } in 
		    let _= SQL.insert  Orderline ol		in  
				let it=SQL.select1 [Item]
		    (fun it -> it.i_id =item.scl_i_id)
		     in 
			   let q=it.i_stock - item.scl_qty in
		    let amt= if( q  < 10) then 21 else 0 in 
					i= i+1;
		    SQL.update Item 
		      (fun i -> i.i_id = item.scl_i_id)
		      (fun i -> i.i_stock <- i.i_stock - item.scl_qty + amt)
				) items in 
				let address= SQL.select1 [Address] 
				 (fun  address -> address.addr_id = addr) in 
						let  	dts = Some (U.gmtime @@ U.time ()) in 
	let  cx= {cx_o_id= o_id   ; cx_type=cc_type; cx_num= cc_number ;cx_name=cc_name ;cx_expiry=cc_expiry; 
	cx_xact_date= dts; cx_xact_amt=total;  cx_co_id= address.addr_co_id }	
	in SQL.insert cx 

	(*
 *Order inquiry transaction
 *)	

	let order_inquiry_txn c_id=
					let orders=SQL.select [Order]
			(fun orders -> orders.o_c_id= c_id) in 
			 let ord = S.max (fun o -> o.o_id) orders in	
		let ord=	SQL.select1 [Customer; Order; Address] 
		   (fun o -> o.o_id=ord.o_id )
			 (fun c-> c.c_id= c_id)
			(fun  a-> fun c-> a.addr_id= c.c_addr_id)  
			(fun o -> fun c -> o.o_c_id=  c.c_id)
			in 
			let ite= SQL.select [Item ; Orderline]
		  (fun  ol-> fun o-> ol.ol_o_id= o.o_id)  
			(fun ol -> fun i -> ol.ol_i_id= i.i_id) in (ite, ord)
	
	(*
 *Best Seller transaction
 *)	
		
			let best_seller_txn subj = 		
				let items=SQL.select [Item ]  
				(fun i -> i.i_subject= subj)
				in  
			let _=	S.foreach ( fun  it-> 
					let ords= SQL.select [Orderline ; Order] 
					   (fun ol -> ol. ol_i_id=it.i_id) 
						  (fun ol-> fun o -> ol.ol_o_id= o.o_id)
						 in 
             let check= match ords with
											| None -> false
      								| Some _ -> true 
											in 
					   if (check==true) 
						 then  begin 		
				    	let  sum_qty= S.sum(fun  ol-> ol.ol_qty) ords	in
				   	let r={r_i_id= it.i_id; r_i_qty=sum_qty  } in 
					  SQL.insert Result r   
						end 
						else () )items in 
			 	let res=SQL.select [Result ]  in 
				let  best= S.max(fun  re-> re.r_i_qty) res	in 
					let info=SQL.select [Item ] 
				   (fun it -> it.i_id=  best.r_i_id) in
				   (info)
				 SQL.delete [Result]
			  		
						

(*
 *New  Product transaction: info about the newest book 
 *)	
				let new_product_txn subj= 
									let items=SQL.select [Item ] 
				(fun items -> items.i_subject= subj) 
				in 
			    let it = S.max (fun it -> it.i_pub_date) items in	
					(it)
	
(*
 *New  Product detail transaction: detail of a specific book
 *)					
					let product_detail_txn i_id =
					let	item= SQL.select1 [Item ; Author]
						(fun  item -> item.i_id=i_id)
						(fun  item-> fun author -> item.i_a_id= author.a_id) in 
						(item)
						
(*		
				let inv1= fun (c:cc_xacts) -> 
					let  ccs= SQL.select [Cc_xacts] in 
				 S.for_all (  fun cc-> 
					let  ord= SQL.select [Order] 
					     (fun  ord-> ord.o_id=cc.cx_o_id)
					in 
				   cc.cx_xact_amt = ord.o_total
					) ccs 
					
				let inv2= fun (i:item) -> 
					let  its= SQL.select [Item] in 
					S.for_all (fun it  ->  
						let a=SQL.select1 [Author]
					   (fun  a-> a.a_id=it.i_a_id) in 
          match (a) with 
		           | Some _ -> true
               | None -> false  ) 
					its
		
		let inv3= fun (c:customer) -> 	
						let  cs= SQL.select [Customer] in 
					S.for_all (fun c  ->  
						let ad=SQL.select1 [Address]
					   (fun  ad-> ad.addr_id=c.c_addr_id) in 
          match (ad) with 
		           | Some _ -> true
               | None -> false  ) 
					cs					
					
			let inv4= fun (o:order) -> 	
						let  ords= SQL.select [Order] in 
					S.for_all (fun ord  ->  
						let c=SQL.select1 [Customer]
					   (fun  c-> c.c_id=ord.o_c_id) in 
          match (c) with 
		           | Some _ -> true
               | None -> false  ) 
					ords					
					
				let inv5= fun (o:order) -> 	
						let  ords= SQL.select [Order] in 
					S.for_all (fun ord  ->  
						let ad=SQL.select1 [Address]
					   (fun  ad-> ad.addr_id=ord.o_ship_addr_id) in 
          match (ad) with 
		           | Some _ -> true
               | None -> false  ) 
					ords	
			
			let inv6= fun (ol:orderline) -> 	
						let  ordls= SQL.select [Orderline] in 
					S.for_all (fun ordl  ->  
						let ord=SQL.select1 [Order]
					   (fun  ord-> ord.o_id=ordl.ol_o_id) in 
          match (ord) with 
		           | Some _ -> true
               | None -> false  ) 
					ordls	
		
								
			let inv7= fun (ol:orderline) -> 	
						let  ordls= SQL.select [Orderline] in 
					S.for_all (fun ordl  ->  
						let it=SQL.select1 [Item]
					   (fun  it-> it.i_id=ordl.ol_i_id) in 
          match (it) with 
		           | Some _ -> true
               | None -> false  ) 
					ordls				
					
		let inv8= fun (cc:cc_xacts) -> 	
						let  ccs= SQL.select [Cc_xacts] in 
					S.for_all (fun cc  ->  
						let ord=SQL.select1 [Order]
					   (fun  ord-> ord.o_id=cc.cx_o_id) in 
          match (ord) with 
		           | Some _ -> true
               | None -> false  ) 
					ccs	
			
		let inv9= fun (cc:cc_xacts) -> 	
						let  ccs= SQL.select [Cc_xacts] in 
					S.for_all (fun cc  ->  
						let ad=SQL.select [Address]
					   (fun  ad-> ad.addr_co_id=cc.cx_co_id) in 
          match (ad) with 
		           | Some _ -> true
               | None -> false  ) 
					ccs	
	*)																	
		
																								
					
				
						
				
						
				 		
				
				
				
			
			

			
				
				 
				 
				
					
				    
			
		
					

			
			  
	
