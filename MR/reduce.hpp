/*
	CUSTOM TYPE TO STORE THE <KEY,VALUE,SOURCE> IN A HEAP
	
	THE SOURCE VECTOR OF THE COUPLE IS ALSO STORED
	IN SUCH A WAY WHEN WE EXTRACT AN ELEMENT IN THE HEAP WE 
	CAN INSERT ANOTHER ITEMS COMING FROM THE SAME VECTOR

*/
template <class Key, class Value>
class triple {
	
public:
	//reference to a key and value
	//plus the id of the mapper
	//that have producted this key value
	Key* k;
	Value* v;
	long bucket;
	
	triple() {};
	triple(Key* _k, Value* _v, long source) {
		k=_k;
		v=_v;
		bucket=source;
	}

	bool operator== (triple &t1) {
		return (*k == *(t1.k));
	}
	
	//NB: is the OPPOSITE becouse the heap is a max heap
	//and i want to use it as a min heap
	bool operator<(const triple &t1) const {
		return (*k > *(t1.k));
	} 

	
};




/*
	REDUCE CLASS (ABSTRACT, THE REDUCE METHOD WILL BE DEFINED BY THE USER)
*/
template <class Key, class Value>
class Reduce {
private:
	//WILL STORE FOR EACH MAP WORKER A VECTOR WITH THE SORTED PARTIAL RESULT 
	//A MIN HEAP WILL BE USED TO FIND THE MINIMUM AMONG THIS VECTORS
	std::vector< std::vector< std::pair<Key*,Value*>>* > *partial_result;
	//A PLACE TO STORE THE RESULT
	std::vector<triple<Key,Value>> result; 
	//MIN HEAP
	std::priority_queue< triple<Key,Value>> heap;
public:


	//doing the reduce phase
	inline void do_reduce(){
		long i, source;
		long mapper= partial_result->size();
		triple<Key,Value> current;
		triple<Key,Value> next;
		triple<Key,Value> aux;
		std::pair<Key*,Value* > temp;
		
		//fill-up the heap with the first element of each mapper partial result
		for (i=0; i<mapper; i++) {
			if ((*partial_result)[i]->size() != 0 ) {
			
				temp= (*partial_result)[i]->back();
				(*partial_result)[i]->pop_back();
				aux.k=temp.first;
				aux.v=temp.second;
				aux.bucket=i;
				heap.push(aux);
				
			}
				
		}
		
		//if i have no items
		if(heap.empty())
			return;
		
		
		//extracting the minimun
		current=heap.top();
		heap.pop();
		source=current.bucket; //origin of the minimum
		
		//get an items from source
		if ((*partial_result)[source]->size() != 0 ) {
			
				temp= (*partial_result)[source]->back();
				(*partial_result)[source]->pop_back();
				aux.k=temp.first;
				aux.v=temp.second;
				aux.bucket=source;
				heap.push(aux);
				
			}
		
		while ( !heap.empty() ) {
			//extracting the minimun
			next=heap.top();
			heap.pop();
			source=next.bucket;//origin of the minimum
			
			//if next has same key of current
			if (next == current) {
				*current.v= reduce(*current.v, *next.v); //do the reduce
				
			} else {
				result.push_back(current);
				current=next;
			}
			
			if ((*partial_result)[source]->size() != 0 ) {
			
				temp= (*partial_result)[source]->back();
				(*partial_result)[source]->pop_back();
				aux.k=temp.first;
				aux.v=temp.second;
				aux.bucket=source;
				heap.push(aux);
				
			}
		}
		result.push_back(current);
		
	};
	
	#ifdef DEBUG
	void debug() {
		triple<Key,Value> t;
		std::cout << "debug reducer\n";
		for (unsigned i=0; i<result.size(); i++) {
			t=result[i];
			(*(t.k)).print();
			std::cout  << " : " << *(t.v) << "\n";
		}
		
	}
	#endif
	
	//ALLOCATION
	inline void setup(long mapper) {
		partial_result = new std::vector<std::vector<std::pair<Key*,Value*>>*>(mapper);
	}
	
	//getting mapper partial result
	inline void linkMapperOut(long mapper_id, std::vector< std::pair<Key*,Value*>>* v) {
		//getting sorted vector of key* value* pair 
		(*partial_result)[mapper_id]=v;
	}
	
	//my output size
	inline long output_size() {
		return result.size();
	}
	
	//FILLING OUTPUT ARRAY STARTING BY START POSITION
	inline void filling_output(std::pair<Key,Value>* output, long start) {
		triple<Key,Value> t;
		std::pair<Key,Value> p;
		long end=result.size() + start;
		long j=0;
		for (long i=start; i<end; i++) {
			t=result[j];
			j++;
			p.first=*(t.k);
			p.second=*(t.v);
			output[i]=p;
		}
		
	}
	
	//DEALLOCATION
	inline void delete_structure() {
		delete partial_result;
	}
	
	/*  USER DEFINED */
	virtual Value reduce(Value v1, Value v2) = 0;
	
};
