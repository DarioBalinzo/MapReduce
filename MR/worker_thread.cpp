
/*ARGUMENTS STRUCTURE FOR WORKER THREAD*/
template <class In, class Key, class Value>
class worker_args {
public:
	long worker_id; //WORKER ID
	In*input; //INPUT REFERENCE
	long input_size;//SIZE OF THE INPUT
	
	std::pair<Key,Value>** output; //reference of where the output's reference will be stored
	long *output_start; //reference of where will be stored the starting position for storing the output
	
	Map<In,Key,Value> *m; //a map class, is nullptr if the worker have not to do the map
	Reduce<Key,Value> *r; //a reduce class, is nullptr if the worker have not do the reduce
	long mapper; // numbers of thread doing the map
	long reducer;//numbers of thread doing the reduce
	
};
 



/*THE WORKER THREAD*/
template<class In, class Key, class Value>
void* worker (void* _arg) {

	//INPUT VALUE PARSING
	worker_args<In,Key,Value>* arg= (worker_args<In,Key,Value> *) _arg;
	long worker_id=arg->worker_id;
	In*input=arg->input;
	long input_size=arg->input_size;
	std::pair<Key,Value>* output;
	long output_start;
	Map<In,Key,Value> *m=arg->m;
	Reduce<Key,Value> *r=arg->r;
	long mapper=arg->mapper;
	long reducer=arg->reducer;
	
	//DO THE MAP (IF I HAVE TO DO IT)
	if (worker_id < mapper) {
		long next;
		long i=0;
		//CALLING THE MAP METHOD ON ALL THE INPUT ELEMENT THAT I HAVE TO PROCESS
		for ( next=worker_id; next<input_size; next=(i*mapper) + worker_id) {
			i++;
			m->map(input[next]); 
			
		}
		//SHUFFLE THE INTERMEDIATE RESULT AND SORT IT FOR SENDING AT RECUDER
		m->shuffle_and_sort();
	}
	
	//WAIT THE END OF MAP PHASE
	pthread_barrier_wait(&barrier);
	
	
	#ifdef DEBUG
	//needed for debugging
	pthread_barrier_wait(&barrier);
	#endif
	
	////DO THE REDUCE (IF I HAVE TO DO IT)
	if(worker_id < reducer) {
		r->do_reduce();
	}
	
	//WAIT END OF THE REDUCE
	pthread_barrier_wait(&barrier);
	//WAIT OUTPUT ALLOCATION
	pthread_barrier_wait(&barrier);
	//FILLING THE OUTPUT ARRAY
	if(worker_id < reducer) {
		output=*(arg->output);
		output_start=*(arg->output_start);
		r->filling_output(output, output_start );
	}

		
	
	
	return nullptr;
}


