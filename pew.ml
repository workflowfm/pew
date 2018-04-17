(* ========================================================================= *)
(* Tools to construct Scala code from Proofs-as-Processes style pi-calculus  *)
(* terms.                                                                    *)
(* Using new stateful library.                                               *)
(*                                                                           *)
(*                   Petros Papapanagiotou, Jacques Fleuriot                 *)
(*              Center of Intelligent Systems and their Applications         *)
(*                           University of Edinburgh                         *)
(*                                    2018                                   *)
(* ========================================================================= *)

needs (!serv_dir ^ "pap/pap.ml");;
needs (!serv_dir ^ "processes/processes.ml");;
needs (!serv_dir ^ "pap/pilib/pilib.ml");;


let rec linprop_to_piobj chanOf prefix tm =
  try 
    let comb,args = strip_comb tm in
      if (is_var comb) then 
	let name = prefix ^ "_a_" ^ (chanOf comb) in
	"Chan(\"" ^ name ^ "\")" 
      else if (comb = `NEG`) then
        let name = prefix ^ "_a_" ^ ((chanOf o hd) args) in
	"Chan(\"" ^ name ^ "\")" 
      else if (comb = `LinTimes`) then
        let lobj = linprop_to_piobj chanOf (prefix ^ "l") (hd args)
	and robj = linprop_to_piobj chanOf (prefix ^ "r") ((hd o tl) args) in
	"PiPair(" ^ lobj ^ "," ^ robj ^ ")"
      else if (comb = `LinPar`) then
        let lobj = linprop_to_piobj chanOf (prefix ^ "l") (hd args)
	and robj = linprop_to_piobj chanOf (prefix ^ "r") ((hd o tl) args) in
	"PiPair(" ^ lobj ^ "," ^ robj ^ ")"
      else if (comb = `LinPlus`) then
        let lobj = linprop_to_piobj chanOf (prefix ^ "l") (hd args)
	and robj = linprop_to_piobj chanOf (prefix ^ "r") ((hd o tl) args) in
	"PiOpt(" ^ lobj ^ "," ^ robj ^ ")"
      else if (comb = `LinWith`) then
        let lobj = linprop_to_piobj chanOf (prefix ^ "l") (hd args)
	and robj = linprop_to_piobj chanOf (prefix ^ "r") ((hd o tl) args) in
	"PiOpt(" ^ lobj ^ "," ^ robj ^ ")"
      else failwith ""
  with Failure s -> failwith ("linprop_to_piobj (" ^ string_of_term(tm) ^ ") :" ^ s);;

let stringNList s i =
  let rec objectListRec s i k = 
    if i > k then [] else (s ^ (string_of_int i))::(objectListRec s (i+1) k) in
  objectListRec s 1 i;;

let rec scala_of_pat t =
  let quot t = "\"" ^ (string_of_term t) ^ "\"" in
  let comb,args = strip_comb t in

  if (comb = `CutProc`) then
    let _ :: c :: cl :: cr :: l :: r :: [] = args in
    "PiCut(" ^ (quot c) ^ "," ^ (quot cl) ^ "," ^ (quot cr) ^ "," ^ (scala_of_pat l) ^ "," ^ (scala_of_pat r) ^ ")"

  else if (comb = `ParProc`) then
    let _ :: _ :: z :: cl :: cr :: c :: [] = args in
    "ParInI(" ^ (quot z) ^ "," ^ (quot cl) ^ "," ^ (quot cr) ^ "," ^ (scala_of_pat c) ^ ")"

  else if (comb = `WithProc`) then
    let _ :: _ :: z :: cl :: cr :: _ :: _ :: l :: r :: [] = args in
    "WithIn(" ^ (quot z) ^ "," ^ (quot cl) ^ "," ^ (quot cr) ^ "," ^ (scala_of_pat l) ^ "," ^ (scala_of_pat r) ^ ")"

  else if (comb = `TimesProc`) then
    let _ :: _ :: z :: cl :: cr :: l :: r :: [] = args in
    "ParOut(" ^ (quot z) ^ "," ^ (quot cl) ^ "," ^ (quot cr) ^ "," ^ (scala_of_pat l) ^ "," ^ (scala_of_pat r) ^ ")"

  else if (comb = `PlusLProc`) then
    let _ :: _ :: z :: cl :: _ :: _ :: c :: [] = args in
    "LeftOut(" ^ (quot z) ^ "," ^ (quot cl) ^ "," ^ (scala_of_pat c) ^ ")"

  else if (comb = `PlusRProc`) then
    let _ :: _ :: z :: cr :: _ :: _ :: c :: [] = args in
    "RightOut(" ^ (quot z) ^ "," ^ (quot cr) ^ "," ^ (scala_of_pat c) ^ ")"

  else if (comb = `IdProc`) then
    let _ :: cl :: cr :: a :: [] = args in
    "PiId(" ^ (quot cl) ^ "," ^ (quot cr) ^ "," ^ (quot a) ^ ")"
     
  else
    "PiCall<(" ^ (quot comb) ^ "," ^ (String.concat "," (map quot (dest_tuple (hd args)))) ^ ")";;


let scala_stateful_proc depth (proc:Proc.t) =
  let fix_param (ty,c) = (string_of_term c, ty) in
  let quot t = "\"" ^ (string_of_term t) ^ "\"" in
  
  let name = proc.Proc.name in
  let ins = map fix_param proc.Proc.inputs
  and out = fix_param proc.Proc.output in
  let toObj i ty = linprop_to_piobj Cllpi.linprop_to_name (name ^ "_" ^ (string_of_int i) ^ "_") ty in
  let toObjPair (i,(c,ty)) = "(" ^ (toObj i ty) ^ ",\"" ^ c ^ "\")" in
  let intypes = (map (fst o scala_type o snd) ins)
  and outtype = (fst o scala_type o snd) out in
  let typarams = string_fun_ty intypes ("Future[" ^ outtype ^"]") in
  let channels = (dest_tuple o rand o lhs) proc.Proc.proc in
  let args = stringNList "o" (length ins) in
  let argToObj (x,c) =
    let ty = (fst o scala_type o assoc c) ins in
    "PiObject.getAs[" ^ ty ^ "](" ^ x ^ ")" in
  let objArgs = String.concat "," (map argToObj (zip args (map fst ins))) in

  let common =
    "override val name = \""^name^"\"" ^ (nltab (depth + 1)) ^
    "override val output = " ^ toObjPair (0,out)  ^ (nltab (depth + 1)) ^
    "override val inputs = Seq(" ^ (String.concat "," (map toObjPair (zip (0--(length ins - 1)) ins))) ^ ")" ^ (nltab (depth + 1)) ^
    "override val channels = Seq(" ^ (String.concat "," (map quot channels)) ^ ")\n" ^ (nltab (depth + 1)) in

  if (proc.Proc.actions = []) then
  "trait "^(String.capitalize name)^" extends ("^typarams^") with AtomicProcess {" ^ (nltab (depth + 1)) ^
  common ^
  "def run(args:Seq[PiObject])(implicit ec:ExecutionContext):Future[PiObject] = args match {" ^ (nltab (depth + 2)) ^
  "case Seq(" ^ (String.concat "," args) ^ ") => this(" ^ objArgs ^ ") map PiObject.apply" ^ (nltab (depth + 1)) ^  
  "}" ^ (nltab depth) ^ "}"
  
  else
  let mk_arg name = String.uncapitalize(name) ^ ":" ^ String.capitalize(name) (*^ "Trait"*)
  and mk_depparam name = String.uncapitalize(name) in
  
  let deps = Proc.get_dep_strings(proc) in
  "class "^(String.capitalize name)^"(" ^ (String.concat "," (map mk_arg deps)) ^ ") extends CompositeProcess { // " ^ typarams ^ (nltab (depth + 1)) ^
  common ^
  "override val dependencies = Seq(" ^ (String.concat "," (map mk_depparam deps)) ^ ")\n" ^ (nltab (depth + 1)) ^
  "override val body = " ^ (scala_of_pat (rhs proc.Proc.proc)) ^ (nltab (depth + 1)) ^ (nltab (depth + 1)) ^
  "def apply(" ^(String.concat "," (map mk_arg intypes))^")(implicit executor:FutureExecutor): Future["^outtype^"] = {" ^ (nltab (depth + 2)) ^
  "implicit val context:ExecutionContext = executor.context" ^ (nltab (depth + 2)) ^  
   "executor.execute(this,Seq("^(String.concat "," (map String.uncapitalize intypes))^")).flatMap(_ map(_.asInstanceOf["^outtype^"]))" ^ (nltab (depth + 1)) ^
  "}" ^ (nltab depth) ^				   
  "}";;



let scala_stateful_proc_file separator path package project (proc:Proc.t) =
  (scala_file_path separator path (package ^ ".processes") (String.capitalize proc.Proc.name),
   "package " ^ package ^ ".processes\n\n" ^
      "import scala.concurrent._\n" ^
      "import com.workflowfm.pew._\n" ^
      "import com.workflowfm.pew.execution._\n" ^
      "import " ^ package ^ "." ^  (String.capitalize project) ^ "Types._\n" ^ (nltab 0) ^ 
      (scala_stateful_proc 0 proc));;
 
let scala_stateful_instance depth name intypes outtype =
  "class "^(String.capitalize name)^"Instance extends "^(String.capitalize name)^" {" ^ (nltab (depth + 1)) ^
    "override def apply( "^ (string_fun_arglist intypes) ^" ) :Future["^outtype^"] = {" ^ (nltab (depth + 2)) ^
    "// TODO: Instantiate this method." ^ (nltab (depth + 1)) ^
    "}" ^ (nltab (depth)) ^
    "}\n";;

let scala_stateful_instance_file separator path package project proc =
  let cll = Proc.get_cll proc in 
  let ins = find_inputs cll
  and out = find_output cll in
    (scala_file_path separator path (package ^ ".instances") ((String.capitalize proc.Proc.name) ^ "Instance"),
    "package " ^ package ^ ".instances\n\n" ^
      "import scala.concurrent._\n" ^
      "import " ^ package ^ "." ^  (String.capitalize project) ^ "Types._\n" ^ 
      "import " ^ package ^ ".processes._\n" ^ (nltab 0) ^ 
    scala_stateful_instance 0 proc.Proc.name (map (fst o scala_type) ins) ((fst o scala_type) out));;

let scala_stateful_main separator path package project proc deps =
  let is_atomic x = x.Proc.actions == [] in
  let atomic,composite =  partition is_atomic deps in
  (*  and depmap = map (fun x -> x.Proc.name,x) deps in *)
  
  let scala_atomic_inst p = "val "^(String.uncapitalize p.Proc.name)^" = new " ^(String.capitalize p.Proc.name)^"Instance"^ (nltab 2)
  and scala_composite_inst p =
    let ldeps = map String.uncapitalize (Proc.get_dep_strings p) in
    "val "^(String.uncapitalize p.Proc.name)^" = new " ^(String.capitalize p.Proc.name)^"("^(string_comma_list ldeps)^")" ^ (nltab 2)
  in
  let params = map (String.uncapitalize o fst o scala_type o rand o rator o invert_ll_term) ((find_input_terms o Proc.get_cll) proc) in
    (scala_file_path separator path package ((String.capitalize proc.Proc.name)^"Main"),
     "package " ^ package ^ "\n\n" ^
       "import scala.concurrent._\n" ^
       "import scala.concurrent.duration._\n" ^
       "import com.workflowfm.pew._\n" ^
       "import com.workflowfm.pew.execution._\n" ^
       "import " ^ package ^ "." ^  (String.capitalize project) ^ "Types._\n" ^ 
       "import " ^ package ^ ".processes._\n" ^ 
       "import " ^ package ^ ".instances._\n" ^ (nltab 0) ^ 
       "object "^(String.capitalize proc.Proc.name)^"Main {" ^ (nltab 1) ^
       "def main(args: Array[String]): Unit = {" ^ (nltab 2) ^
       (itlist (^) (map scala_atomic_inst atomic) (nltab 2)) ^
       (itlist (^) (map scala_composite_inst composite) (nltab 2)) ^
       (scala_composite_inst proc) ^ (nltab 2) ^ 
       "implicit val executor:FutureExecutor = SimpleProcessExecutor()\n" ^ (nltab 2) ^
       "// TODO: Provide actual parameters:" ^ (nltab 2) ^
       "val result = Await.result("^(String.uncapitalize proc.Proc.name)^"( "^(string_comma_list params)^" ),30.seconds)" ^ (nltab 1) ^
       "}" ^ (nltab 0) ^
       "}");;

let scala_stateful_deploy separator path package project proc deps main java =
  let is_atomic x = x.Proc.actions == [] in
  let atomic = filter is_atomic deps in 
  let depmap = map (fun x -> x.Proc.name,x) deps in
  let depfiles = map (scala_stateful_proc_file separator path package project) (proc :: deps)
  and javarunner = if (java && main) then [java_runner separator path package project proc.Proc.name] else []
  and instances = map (scala_stateful_instance_file separator path package project) atomic
  and typesobject = [scala_types_object separator path package project (proc :: deps)]
  and mainclass = if (main) then [scala_stateful_main separator path package project proc deps] else []
  in
    (depfiles @ javarunner , instances @ typesobject @ mainclass);;

(* (overwrite,optional) *)


let scala_stateful_deploy_print separator path package project proc deps main java =
  let overwrite,optional = scala_stateful_deploy separator path package project proc deps main java
  and print_file (addr,content) = print_string ("[" ^ addr ^ "]\n" ^ content ^ "\n\n") in
    print_string "--- OVERWRITE ---\n\n" ;
    map print_file overwrite ;
    print_string "--- OPTIONAL ---\n\n" ;
    map print_file optional ;;

(*
scala_stateful_deploy_print "/" "/home/kane/PapPiLib/" "uk.ac.ed.skiexample" "SkiExample" getSki [selectModel;selectLength;cm2Inch;usd2Nok;selectSki] true true;;
*)
