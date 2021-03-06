//function used to sort the key when stored for the reducers workers
//sort in reverse order, so the reducer will find the minimum with pop()
template <class Key, class Value>
bool compare(std::pair<Key*,Value*> p1, std::pair<Key*,Value*> p2) {
		//compare only the key (reverse the order!!!)
		return *(p1.first) > *(p2.first);
	}

/*
	THE MAP CLASS (ABSTRACT, USER WILL DEFINE MAP METHOD)


*/
template <class In, class Key, class Value>
class Map {

private:
	std::unordered_map<Key,Value> *table; //hash table
	Reduce<Key,Value> *r; //reference to a reduce class, used only for applying reduce operator
	long reducer;
	 //storing ordered partial result to each reducer
	std::vector<std::vector<std::pair<Key*,Value*>>> *partial_result;
	
	
public:	

	//THE EMIT METHOD
	inline void emit(Key *key, Value *val) {
		if (table->count(*key)>0) {
			//doing the reduce, key was already present
   	 		table->at(*key)=r->reduce(table->at(*key), *val);   
  		} else {
  			//inserting for the first time a key
   			(*table)[*key]=*val; 
   		}
		return;
	};
	
	//ALLOCATION STARTUP
	inline void setup(long size, Reduce<Key,Value>* _r, long _reducer ) {
		//allocate the hash table of initial size "size"
		reducer=_reducer;
		table=new std::unordered_map<Key,Value>;
		table->rehash(size);
		
		partial_result=new std::vector<std::vector<std::pair<Key*,Value*>>>(reducer); 
		//set reference to reduce class
		r=_r;
		
	}
	
	#ifdef DEBUG
	void debug() {
		std::cout << "myrecipe contains:" << std::endl;
  		for (auto& x: *table) {
  			x.first.print();
   			std::cout << ": " << x.second << std::endl;
   		}
   		std::cout << std::endl;
   		long i;
   		for(i=0; i<reducer; i++) {
   			std::cout << ">>>>todest " << i << "\n";
   			int j;
   			for (j=0; j<(*partial_result)[i].size(); j++) {
   				(*((*partial_result)[i][j].first)).print();
   				std::cout << ">>>>" << " : " << *((*partial_result)[i][j].second) << "\n";
   			}
   		}
   		
	}
	#endif
	
	
	//DEALLOCATION
	inline void delete_table() {
		delete table;
		delete partial_result;
	}
	
	
	//SHUFFLE AND SORT, END OF THE MAP PHASE
	inline void shuffle_and_sort() {
		//shuffle
		long i;
		for (auto& x: *table) {
			long destination=std::hash<Key>()(x.first) % reducer;
   			(*partial_result)[ destination ].push_back(std::pair<Key*,Value*>((Key*) &(x.first),&(x.second)) );
   		}
   		//sort
   		
   		for (i=0; i<reducer; i++) {
   			// using function as comp
  			std::sort((*partial_result)[i].begin(), (*partial_result)[i].end(), compare<Key,Value>);
   		}
   		
		
	}
	
	
	inline long getItemsNumberTo(long reducer_id) {
		return (*partial_result)[reducer_id].size();
	}
	
	inline std::vector<std::pair<Key*,Value*>> * getPartial(long reducer_id) {
	
		return & ((*partial_result)[reducer_id]);
	}
	
	
	/*
	* 			USER DEFINED
	*/
	virtual void map(In input) = 0;
	
};
