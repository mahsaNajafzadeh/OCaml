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
   |Broker
	| Customer
	| Customer_account
	| Account_permission
	| Trade_history
	| Trade_request
	| Trade
	| Trade_type
	| Last_trade
	| Holding
	| Holding_history
	| Holding_summery
	| Status_type
	| Settlement
	| Exchange
	| Security
	| Company
	| Taxrate
	| Customer_taxrate
	| Commission_rate
	| Result 




type broker = {b_id:id; mutable b_num_trades: int; mutable b_comm_total: int}
type customer={c_id: id;c_tax_id : id;  c_tier:int }
type customer_account={ca_id:id; ca_b_id:id; ca_c_id:id; mutable ca_bal: int ; mutable ca_tax_st: int}
type trade={t_id:id; mutable t_dts:Unix.tm option;  mutable  t_st_id: string ; mutable t_price: int; mutable
 t_ca_id: id; mutable t_qty :int ;mutable t_tr_type:string; mutable t_symb:string ;
mutable t_tax: int ; mutable t_comm: int ; mutable t_exec_name: string ;mutable t_is_cash: int  }
type last_trade={ lt_s_symb: string; mutable lt_price: int; mutable lt_vol: int; mutable lt_dts : Unix.tm option}
type trade_request={ tr_t_id: id; mutable tr_tt_id: string; mutable tr_s_symb: string;
 mutable tr_qty: int; mutable tr_bid_price: int; mutable tr_ca_id: id }
type trade_history={th_t_id: id; mutable th_dts: Unix.tm option ; mutable th_st_id:string}
type security={s_symb: string ; s_ex_id :string ; s_co_id: id}
type holding= {h_t_id: id; h_s_symb:string; h_ca_id:id; mutable h_price:int ; mutable h_qty:int }
type holding_summery={hs_ca_id:id; hs_s_symb:string; mutable hs_qty: int}
type holding_history={hh_h_t_id:id; hh_t_id:id; mutable hh_before_qty: int; mutable hh_after_qty:int}
type status_type={st_id:string; st_name: string}
type account_permission={ap_ca_id:id; ap_acl: string;  ap_tax_id :id } 
type trade_type= {tt_id: string ; mutable tt_is_sell: int; mutable tt_is_mrkt: int }
type settlement={se_t_id : id;mutable se_cash_type : string; mutable se_amt : int }
type exchange ={ex_id: string;  ex_name:string  }
type company={co_id: id; mutable  co_st_id: string;  co_name: string}
type taxrate={tx_id: string; tx_name: string; mutable tx_rate: int}
type customer_taxrate={cx_tx_id: string ; cx_c_id: id}
type commision_rate={cr_c_tier: int; cr_tt_id: string;cr_ex_id:string;cr_from_qty:int;
 mutable cr_rate:int; mutable cr_to_qty: int }
type result={mutable h_id: id; mutable buy: int ; mutable sell: int ; mutable needed: int}


(*
 * Trade Status (read-only) transaction.
 *)		
	let trade_status_txn  acct_id=
		let ts=SQL.select [Trade; Status_type ; Trade_type ; Security; Exchange] 
		   (fun t -> t.t_ca_id= acct_id )
		  (fun t -> fun s -> t.t_st_id= s.st_id)
			(fun t-> fun tt -> t.t_tr_type= tt.tt_id) 
			(fun t-> fun se -> t.t_symb= se.s_symb)
			 (fun se-> fun ex -> se.s_ex_id= ex.ex_id)
			in ( S.foreach ( fun t -> 
				print  t.t_id ;
				 ) ts )
				
(*
 * Trade lookup (read-only) transaction.
 *)		

	let trade_lookup_txn frame_to_execute max_trades  acct_id  symbol start_trade_dts end_trade_dts (trade_ids: id list)= 
		if(frame_to_execute ==1) 
		 then begin
			   let  i=0 in 
		    while  (i!=max_trades)  do 
			    	let t= SQL.select1 [Trade] 
	       (fun t -> t.t_id=List.nth  trade_ids  i) in 
				    let  se= SQL.select1 [Settlement] 
	       (fun se -> se.se_t_id=List.nth  trade_ids  i) in 
				  SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id=List.nth  trade_ids  i)
				 i= i+1;
				done;
			end	
			else if (frame_to_execute ==2 ) 
			then begin  
				let trades= SQL.select [Trade] 
	       (fun t -> t.t_ca_id=acct_id) 
				 (fun t -> t.t_st_id="CMPT") 
				 (fun t -> t.t_dts >= start_trade_dts &&t.t_dts <= end_trade_dts) 
				in S.foreach(fun trade->  
				SQL.select1 [Settlement] 
				  (fun se -> se.se_t_id=trade.t_id) ;
					SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id=trade.t_id)
					)trades 
					end
			else if (frame_to_execute ==3 ) 
			 then begin  
				let trades= SQL.select [Trade] 
	       (fun t -> t.t_symb=symbol) 
				 (fun t -> t.t_st_id="CMPT") 
				 (fun t -> t.t_dts >= start_trade_dts &&t.t_dts <= end_trade_dts)  in 
				 S.foreach(fun trade->  
				 SQL.select1 [Settlement] 
				  (fun se -> se.se_t_id=trade.t_id) ;
					SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id=trade.t_id)
					)trades 
					end
			else if (frame_to_execute ==4 ) 
			 then begin  
				let trade= SQL.select1 [Trade] 
	       (fun t -> t.t_ca_id=acct_id) 
				 (fun t -> t.t_dts >= start_trade_dts) 	in
					    let check= match trade with
											| None -> false
      								| _ -> true 
											in 
					   if (check==true) 
						 then  
							SQL.select1 [Trade_history] 
	              (fun th -> th.th_t_id=trade.t_id);
					end
							
	
(*
 *Market Feed transaction
 *)

(*// let's do the updates first ,   // then, see about pending trades *)

let market_feed_txn  lt_s_symb  t_price tr_qty   t_st_id  type_stop_loss type_limit_sell type_limit_buy= 
	 let  	dts = Some (U.gmtime @@ U.time ())  in 
	(*let _=while  (i!=max_feed_len)  do  
		i= i+1;*)
  let _= SQL.update Last_trade   
	  (fun lt ->
			 begin 
			  lt.lt_vol <- tr_qty;
			  lt.lt_price <- t_price;
			  lt.lt_dts <- dts  
			end)
		(fun lt -> lt.lt_s_symb = lt_s_symb)   in
	(*	done in 
	let j=0 in 
	while  (j!=max_feed_len)  do *)
		let trss= SQL.select [Trade_request] 
		    (fun trs -> ( trs.tr_s_symb=lt_s_symb  &&  
				   trs.tr_tt_id = type_stop_loss  && trs.tr_bid_price >= t_price) || 
				  ( trs.tr_s_symb=lt_s_symb  &&  trs.tr_tt_id = type_limit_sell  && trs.tr_bid_price <= t_price) ||
					( trs.tr_s_symb=lt_s_symb  && trs.tr_tt_id = type_limit_buy  && trs.tr_bid_price >= t_price)
					) 
					in 
   S.foreach (  fun trs  -> 
	 let _ = SQL.update Trade
				(fun t->
					    begin
								 t.t_dts=dts;
								 t.t_st_id= t_st_id ;
								 end)
        (fun t -> t.t_id = trs.tr_t_id) in 
		let _= SQL.delete Trade_request 
          (fun tr -> tr.tr_t_id = trs.tr_t_id)  in 
		let  th={th_t_id= trs.tr_t_id ; th_dts= dts ; th_st_id=t_st_id }	in 
	        	SQL.insert Trade_history	th 
						()				
						) trss
		(*	j= j+1;			
			done *)
						
(*
 *Trade order transaction
 *)
let trade_order_txn acct_id  symbol t_id t_qty trade_type_id  
    exec_tax_id t_qty t_tr_type requested_price st_pending_id st_submitted_id co_name
		 exec_name is_cash= 
	let ca=SQL.select1 [Customer_account] 
	   (fun ca -> ca.ca_id= acct_id)   in 
		let tax_status= ca.ca_tax_st in  
		let cust_id=ca.ca_c_id in 
		let c=SQL.select1 [Customer] 
	    (fun c -> c.c_id=ca.ca_c_id ) in 
			let cust_tier= c.c_tier in
		 let b=SQL.select1 [Broker]  
		   (fun b -> b.b_id=ca.ca_b_id )  in 
				let x=	if (exec_tax_id != c.c_tax_id) 
						 	then  1 
							else  0 in 
				  let  acl=  SQL.select1 [Account_permission]  
	   					  	(fun acl -> acl.ap_ca_id=acct_id && acl.ap_tax_id= exec_tax_id ) in 
 			     let check= match (x,acl) with
    								  | (0, None) -> true
											| (0, Some _) -> true
      								| (1, Some _) -> true 
											| (1, None) -> failwith "ACL for a Trade-Order transaction is NULL"
											| _ ->  failwith "ACL for a Trade-Order transaction is NULL"
								in   	
							  if(check== true  ) 
												  then 
														begin
													let exch_id="" in		
													let _=	if (symbol=="")	
															then 
																begin
					                    let co= SQL.select1 [Company] 
													     (fun co -> co.co_name =co_name ) in 
															let s= SQL.select1 [Security] 
													     (fun s -> s.s_co_id =co.co_id ) in
															exch_id=s.s_ex_id
															end
														 else 																
														 let s= SQL.select1 [Security] 
													   (fun s -> s.s_symb=symbol ) in 
														  exch_id=s.s_ex_id
														in 												
														 let lt= SQL.select1 [Last_trade] 
													        	(fun lt -> lt.lt_s_symb=symbol )	
														in 
																let  tt= SQL.select1 [Trade_type] 
													       (fun tt -> tt.tt_id=trade_type_id ) in 
																let type_is_sell=tt.tt_is_sell in
															let  hs= SQL.select1 [Holding_summery] 
													       (fun hs -> hs.hs_ca_id=acct_id  && hs.hs_s_symb=symbol)
														in		 
														 let requested_price=if (tt.tt_is_mrkt == 1) then
															    lt.lt_price
															else requested_price in 
														(*	 let buy_value = 0 in
       												 let sell_value = 0 in 
    													 let	needed_qty = t_qty in *)
															 let hs_qty= match (hs) with
    						  								| None -> 0
																	| Some _ -> hs.hs_qty in		  
						let _= if (type_is_sell == 1) 
													then	begin 
											if(hs_qty > 0) 
													then  begin
							           	let ehs=SQL.select [Holding] 
								         (fun ehs -> ehs.h_ca_id = acct_id &&
                            ehs.h_s_symb=symbol) in 
								 let _= SQL.delete [Result]	 in 													
							S.foreach (
								fun eh -> begin 
									let ress= SQL.select [Result]	
									 in 
									let needed_qty=match (ress) with 
									| None -> t_qty 
									| Some _ -> S.min (fun n->n.needed ) ress		 in 	
									if (needed_qty !=0) then begin		
											  if(eh.h_qty >needed_qty) then begin											
												let res={h_id= eh.h_t_id; buy= needed_qty * eh.h_price;
												   sell= needed_qty * requested_price;  needed=0} in 
												SQL.insert Result	res 
											end 		
											else 
												let res={h_id= eh.h_t_id; buy= eh.h_qty * eh.h_price;
												   sell= eh.h_qty * requested_price;  needed=needed_qty - eh.h_qty} in 
												SQL.insert Result	res 
												 end
												end	)	ehs 	  
									    end													
					(*					let buy_value=	S.sum (fun eh ->   if(eh.h_qty >needed_qty) then
											 needed_qty * eh.h_price  else eh.h_qty * eh.h_price ) ehs
										in 
										let sell_value=	S.sum (fun eh ->   if(eh.h_qty >needed_qty) then
											 needed_qty * requested_price  else eh.h_qty * requested_price
											 ) ehs in
											let needed_qty= if(eh.h_qty >needed_qty) then 0 else 
												  needed_qty - eh.h_qty;								  	
												S.foreach (
								           fun eh -> begin 
										   if(eh.h_qty >needed_qty)
									      then 
									    	begin
											  buy_value = buy_value + needed_qty * eh.h_price;
											  sell_value = sell_value + needed_qty * requested_price; 
											  needed_qty =0; 
												let x=x in ()
											  end 	 
										    else 
											  begin
											  buy_value = buy_value + eh.h_qty * eh.h_price;
											  sell_value = sell_value + eh.h_qty * requested_price; 
											  needed_qty =needed_qty - eh.h_qty;
												let x=x in ()
										    end	
							           end	)	ehs 	  
									    end  *)
					end
					else
					 begin
       let _=  if(hs_qty<0)
				then
					begin 
								let ehs=SQL.select [Holding] 
								   (fun ehs -> ehs.h_ca_id = acct_id &&
                         ehs.h_s_symb=symbol)		in		
							 let _= SQL.delete [Result]	 in 												
							S.foreach (
								fun eh -> begin 
									 let ress= SQL.select [Result]	
									 in 
									let needed_qty=match (ress) with 
									| None -> t_qty
									| Some _ -> S.min (fun n->n.needed ) ress		 in 
									if (needed_qty !=0) then begin	
									let hold_qty= eh.h_qty in 
									let hold_price= eh.h_price in 
									if(hold_qty +needed_qty <0 )
									 then 
										begin
												let res={h_id= eh.h_t_id; buy= needed_qty * requested_price;
												  sell= needed_qty * eh.h_price;  needed=0} in 
												SQL.insert Result	res 
											end 	 
										else 
											begin
										let res={h_id= eh.h_t_id; buy=  -hold_qty * requested_price;
												  sell= -hold_qty * hold_price;  needed=needed_qty- -hold_qty} in 
												SQL.insert Result	res 
										end
										end
										else ()	
							 end	)	ehs 
										end 		  
										else () in ()
										end		in 
									let res=	SQL.select [Result] in 
			let buy_value= S.sum	(fun res -> res.buy) res in 
			 let sell_value= S.sum	(fun res -> res.sell) res		in 			
			let tax_amount= if (sell_value >buy_value && (tax_status == 1 || tax_status == 2) )
													    then 
														begin 
													 let		taxes=		SQL.select [Taxrate; Customer_taxrate] 
													 ( fun tx -> fun ct -> tx.tx_id=ct.cx_tx_id )
														(fun ct-> ct.cx_c_id= cust_id) in 
														 S.sum (fun tx -> tx.tx_rate) taxes 
																	end
																	else 
																		0 in
													let cr=SQL.select1 [Commission_rate] 
													 ( fun cr -> cr.cr_c_tier =cust_tier  && cr.cr_tt_id=trade_type_id
													  && cr.cr_ex_id=exch_id )
														(fun cr -> cr.cr_from_qty <= t_qty && cr.cr_to_qty >= t_qty) in
													let comm_rate =cr. cr_rate in 
		                           let 	comm_amount = (comm_rate / 100) * t_qty * requested_price in 
														  let  t_price=  if (tt.tt_is_mrkt == 1) then 
															                  lt.lt_price else requested_price in		
															  let tr={tr_t_id=t_id; tr_tt_id=t_tr_type; 
																tr_s_symb=symbol; tr_qty=t_qty; tr_bid_price=t_price; 
																tr_ca_id=acct_id} in										 
														  let  t_st_id = if (tt.tt_is_mrkt  == 1) then st_submitted_id
															              else st_pending_id in	
															let  	t_dts = Some (U.gmtime @@ U.time ())  in 																
														   let t= {t_id; t_dts; t_st_id ; t_price; t_ca_id=acct_id; t_qty;
																		 t_tr_type ; t_symb=symbol; t_tax=0;t_comm=comm_amount; 
																			t_exec_name=exec_name;
																				t_is_cash=is_cash
																		 } in 
																 let tr_h={th_t_id=t_id; th_dts=t_dts; th_st_id= t_st_id }	in		
														      let _=SQL.insert Trade t in
																  if (tt.tt_is_mrkt == 0)
																   then  SQL.insert Trade_request tr 
																	else () ;  		
																  SQL.insert Trade_history tr_h;
															 end
															SQL.delete [Result]	 

	(*
 *Trade update transaction
 *)																								  

 let trade_update_txn frame_to_execute max_trades  end_trade_dts start_trade_dts
acct_id  symbol max_updates t_id=
 (*frame 1: collecting info
        // info about the trade *) 
		if(frame_to_execute ==1) 
		then
			begin
		(* let num_updated = 0 in
		 let  i=0 in (trade_ids: id list) 
		 while  (i!=max_trades)  do  *)
			(*if(num_updated < max_updates) 
			then begin*) 
			 	let t= SQL.select1 [Trade] 
	       (fun t -> t.t_id=t_id) in
				 let ex= match  t.t_exec_name with 
				|"some _ X some _" -> "some _  some _"
				| _ -> "some _ X some _" in
				let _= SQL.update  Trade 
				(fun f -> f.t_exec_name <- ex) 
				(fun f -> f.t_id = t_id) in 
				 let trade= SQL.select1 [Trade_type ; Trade] 
	       (fun tt -> fun t -> tt.tt_id =t.t_tr_type)
				 (fun t -> t.t_id =t_id)
				 in
					let se= SQL.select1 [Settlement] 
	       (fun se -> se.se_t_id =t_id) in			
				 let th= SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id =t_id) in ()
	(*			i =i +1; 
			done;	*)
			end
			else if (frame_to_execute == 2) 
			then begin 
					let ts= SQL.select [Trade] 
	       (fun ts -> ts.t_ca_id=acct_id && ts.t_dts >= start_trade_dts && ts.t_dts <=end_trade_dts
				  && ts.t_st_id="CMPT") in 
		(*		let num_updated : int ref = ref 0 in *)
				S.foreach ( begin 
					fun t ->	
					let trade_id= t.t_id   in 
				  	(*let _= if (!num_updated < max_updates) then 
						begin  *)
						let  s= SQL.select1 [Settlement] 
					   (fun s ->s.se_t_id = trade_id ) in 
			(*			let cash_type= s.se_cash_type in *)
					let cash_type=	if( t.t_is_cash==1  )
						 then begin 
					     if (s.se_cash_type =="Cash Account") 
							    then "Cash"
								else "Cash Account"  
							end 
							else 
								begin
						    	if (s.se_cash_type =="Cash Margin") 
							    then "Margin"
								else "Cash Margin" 
								end in							
				let _=SQL.update  Settlement 
				(fun se -> se.se_cash_type <- cash_type) 
				(fun se -> se.t_id=trade_id)  in 
			(*	  num_updated := !num_updated +  1*)
					(*end 
					else  () in  *)
				  SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id =trade_id ); 
        end ) ts 
				end
			else  if(frame_to_execute == 3)
			then
			 begin
					let trades= SQL.select [Trade ; Trade_type ; Security] 
	       (fun tr ->  tr.t_symb=symbol  &&
				  tr.t_st_id ="CMPT" && tr.t_dts >= start_trade_dts && tr.t_dts <= 
						end_trade_dts ) 
					(fun tr -> fun  tt -> tt.tt_id= tr.t_tr_type )	
					(fun tr -> fun se -> se.s_symb= tr.t_symb) in			 
		   S.foreach ( fun trade -> 
									let  s= SQL.select1 [Settlement] 
					   (fun s ->s.se_t_id = trade.t_id ) in  
										SQL.select1 [Trade_history] 
	       (fun th -> th.th_t_id =trade.t_id ))  trades 
				end 
				else ()
				
				
let trade_result_txn t_id trade_price  st_completed_id= 
	let t= SQL.select1 [Trade] 
	 (fun t -> t.t_id=t_id) in  
	let tt= SQL.select1 [Trade_type] 
	  (fun tt-> tt.tt_id=t.t_tr_type) in 
			 let h= SQL.select1 [Holding_summery] 
	  (fun h-> h.hs_ca_id= t.t_ca_id && h.hs_s_symb=t.t_symb ) in
		 	let hs_qty= match (h) with
    						| None -> 0
								| Some _ -> h.hs_qty in
		let c= SQL.select1 [Customer_account] 
	  (fun c-> c.ca_id=t.t_ca_id ) in 
		(*let needed_qty = t.t_qty in*)
		(*let buy_value = 0  in 
		let  sell_value =0 in  *)
	 let _=  if(tt.tt_is_sell==1)
	    then
				begin 
		 let _= 	if(hs_qty == 0)  
				then 
					begin
				let hs={hs_ca_id=t.t_ca_id;  hs_s_symb=t.t_symb; hs_qty=(-t.t_qty)} in 
				 SQL.insert Holding_summery hs;
				end  
			else if(hs_qty != t.t_qty)
					 then 
						begin
						   SQL.update Holding_summery
                (fun s -> 
                     s.hs_qty <- hs_qty - t.t_qty) 
                (fun s -> s.hs_ca_id  = t.t_ca_id &&
                         s.hs_s_symb=t.t_symb)
						 end
						else if (hs_qty > 0  ) then 
								begin
								let ehs=SQL.select [Holding] 
								   (fun ehs -> ehs.h_ca_id = t.t_ca_id &&
                         ehs.h_s_symb=t.t_symb)		in		
								 let _= SQL.delete [Result]	 in 													
							S.foreach (
								fun eh -> begin 
									let ress= SQL.select [Result]	
									 in 
									let needed_qty=match (ress) with 
									| None -> t.t_qty
									| Some _ -> S.min (fun n->n.needed ) ress		 in 	
									if (needed_qty !=0) then begin	
									if(eh.h_qty >needed_qty)
									 then 
										begin
										 let hh={hh_h_t_id=eh.h_t_id; hh_t_id=t.t_id; hh_before_qty=eh.h_qty; hh_after_qty=eh.h_qty-needed_qty}
										 in SQL.insert  Holding_history hh;
											let _=SQL.update  Holding 
											    (fun f ->  f.h_qty <-  eh.h_qty - needed_qty)
													( fun f -> f.h_t_id= eh.h_t_id) in 												
												let res={h_id= eh.h_t_id; buy= needed_qty * eh.h_price;
												   sell= needed_qty * trade_price;  needed=0} in 
												SQL.insert Result	res 
											end 	 
										else 
											begin
									let hh={hh_h_t_id=eh.h_t_id; hh_t_id=t.t_id; hh_before_qty=eh.h_qty; hh_after_qty=0}
										 in SQL.insert  Holding_history hh;
											let _=	SQL.delete  [Holding] 
											 (fun f -> f.h_t_id= eh.h_t_id) in 
												let res={h_id= eh.h_t_id; buy= needed_qty * eh.h_price;
												   sell= needed_qty - trade_price;  needed=needed_qty- eh.h_qty} in 
												SQL.insert Result	res 
										end	
										end
										else ()
							 end	)	ehs end
							in 
	   		let ress= SQL.select [Result]	
					 in 
			  let res_qty=match (ress) with 
						  | None -> t.t_qty
				      | Some _ -> S.min (fun n->n.needed ) ress		 in 					
				 if (res_qty >0) 
				 then 
					begin 
					let hhh={hh_h_t_id=t.t_id; hh_t_id=t.t_id; hh_before_qty=0; 
					hh_after_qty= -res_qty}
										 in SQL.insert  Holding_history hhh;
				 let hhhh={h_t_id=t.t_id ; h_ca_id=t.t_ca_id;  h_s_symb=t.t_symb;  h_price=trade_price;h_qty= -res_qty}
										 in SQL.insert  Holding hhhh;					
					end 
				  else if(hs_qty ==  t.t_qty) 
					then
					  begin
								SQL.delete  [Holding_summery] 
							 (fun f -> f.hs_ca_id =t.t_ca_id && f.hs_s_symb=t.t_symb)
							end	
				end
				else
					 begin
       let _=  if(hs_qty==0)
				then
					begin 
				 let hs={hs_ca_id=t.t_ca_id;  hs_s_symb=t.t_symb; hs_qty=t.t_qty} in 
				 SQL.insert  Holding_summery hs
				 end 
				 else if (-h.hs_qty != t.t_qty) 
					 then 
						begin
						   SQL.update Holding_summery
                (fun s -> 
                     s.hs_qty <-  h.hs_qty + t.t_qty)
                (fun s -> s.hs_ca_id  = t.t_ca_id &&
                         s.hs_s_symb=t.t_symb)
							end
						else if (hs_qty < 0)	
						 then begin 
								let ehs=SQL.select [Holding] 
								   (fun ehs -> ehs.h_ca_id = t.t_ca_id &&
                         ehs.h_s_symb=t.t_symb)		in		
							 let _= SQL.delete [Result]	 in 												
							S.foreach (
								fun eh -> begin 
									 let ress= SQL.select [Result]	
									 in 
									let needed_qty=match (ress) with 
									| None -> t.t_qty
									| Some _ -> S.min (fun n->n.needed ) ress		 in 
									if (needed_qty !=0) then begin	
									let hold_qty= eh.h_qty in 
									let hold_price= eh.h_price in 
									if(hold_qty +needed_qty <0 )
									 then 
										begin
										 let hh={hh_h_t_id=eh.h_t_id; hh_t_id=t.t_id; hh_before_qty=eh.h_qty; hh_after_qty=eh.h_qty+needed_qty}
										 in SQL.insert  Holding_history hh;
											let _= SQL.update  Holding 
											    (fun f ->  f.h_qty <-  eh.h_qty + needed_qty)
													( fun f -> f.h_t_id= eh.h_t_id) in 
												let res={h_id= eh.h_t_id; buy= needed_qty * eh.h_price;
												  sell= needed_qty * trade_price;  needed=0} in 
												SQL.insert Result	res 
											end 	 
										else 
											begin
									let hh={hh_h_t_id=eh.h_t_id; hh_t_id=t.t_id; hh_before_qty=hold_qty; hh_after_qty=0}
										 in SQL.insert  Holding_history hh;
												SQL.delete  [Holding] 
											 (fun f -> f.h_t_id= eh.h_t_id);	
											hold_qty = -hold_qty;
										let res={h_id= eh.h_t_id; buy=  hold_qty * trade_price;
												  sell= hold_qty * hold_price;  needed=needed_qty- hold_qty} in 
												SQL.insert Result	res 
										end	
										end
										else ()
							 end	)	ehs 
							end 
							in 
	   		let ress= SQL.select [Result]	
					 in 
			  let res_qty=match (ress) with 
						  | None -> t.t_qty
				      | Some _ -> S.min (fun n->n.needed ) ress		 in 					
				 if (res_qty >0) 			
				 then 
					begin 
					let hhh={hh_h_t_id=t.t_id; hh_t_id=t.t_id; hh_before_qty=0; 
					hh_after_qty= -res_qty}
										 in SQL.insert  Holding_history hhh;
				 let hhhh={h_t_id=t.t_id ; h_ca_id=t.t_ca_id;  h_s_symb=t.t_symb;  h_price=trade_price;h_qty= res_qty}
										 in SQL.insert  Holding hhhh;					
					end 
				  else if( -hs_qty == t.t_qty) 
					then
					  begin
								SQL.delete  [Holding_summery] 
							 (fun f -> f.hs_ca_id =t.t_ca_id && f.hs_s_symb=t.t_symb)
							end	
				end	in  
				SQL.delete [Result] 
							
						
	(*	let 	inv3= fun (hs :holding_summery)  ->
		   let holdings= SQL.select [Holding]  
			    (fun h -> h.h_ca_id= hs.hs_ca_id &&  h .h_s_symb=hs.hs_s_symb) in
				let  sum_h_qty= S.sum(fun h -> h.h_qty) holdings 
				 in   hs.hs_qty =sum_h_qty
				
				  let inv2 =	fun (b:broker) ->
					   let trades=  SQL.select [Customer_account ; Trade]
						    (fun t -> fun ca ->  t.t_ca_id=c.ca_id    )  
								 ( fun ca  -> ca.ca_b_id = b.b_id ) 
							(fun t -> 	t.t_st_id= "CMPT")
								 in	
         let  sum_t_comm= S.sum(fun t -> t.t_comm) trades 
				 in   b.b_comm_total =sum_t_comm
				
			let inv1= fun (b: broker) ->
			  let trades=  SQL.select [Customer_account ; Trade]
						    (fun t -> fun ca ->  t.t_ca_id=c.ca_id    )  
								 ( fun ca  -> ca.ca_b_id = b.b_id ) 
							(fun t -> 	t.t_st_id= "CMPT")
								 in	
         let  count= S.size trades 
				 in   b.b_num_trades =count
				*)
				
				
				
				
																										
	



																

													
													
									
		       


				
											
													
			 
				
				
				
				
				
				
