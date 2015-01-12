#include "skeleton.hpp"
#include "string.hpp"

#define M 3
#define R 3

class myint {
	public:
		int x;
		myint() {
			std::cout << "creating__int\n";
		}
		~myint() {
			std::cout << "deleting__int " << x<< "\n";
		}
		
		bool operator==(const myint &other) const
  { 
  	return (x==other.x);
  }
}; 


namespace std {

  template <>
  struct hash<myint>
  {
    std::size_t operator()(const myint& k) const
    {
      using std::size_t;
      using std::hash;
      using std::string;


      return ((hash<int>()(k.x)));
    }
  };

}

class MyMap : public Map<int,int,int> {
	public:
		int a;
		int b;
		
		
		void map(int input) {
		
			
			
			
			b=input;
			int i;
			for (i=0; i<12; i++) {
				a=(input%2)+i;
				emit(&a,&b);
			}
		}
	
};


class MyReduce : public Reduce<int,int> {


		int reduce(int a, int b) {
			return a+b;
		}
		
		
};

int main() {

	
	
	
	int a[10];
	line b[10];//dummy testing
	
	std::pair<line,int> dummyarray[10];
	
	std::unordered_map<std::string,std::string> table;
	table["ciao"]="wow";
	table["ciaociao"]="wow";
	std::cout << table.at("ciao") << "\n"; 
	
	try {
   	 	std::cout << table.at("ciaociao") << "\n";      //throws an out-of-range
  	}
  		catch (const std::out_of_range& oor) {
   		table["ciaociao"]="wowwow";
		std::cout << table.at("ciaociao") << "\n"; 
   		 
  	}
	
	
	
	
	
	int i;
	for(i=0; i<10; i++) {
		a[i]=i;
	}
	std::vector<Map<int,int,int>*> m;
	std::vector<Reduce<int,int>*> r;
	for(i=0; i<M; i++) {
		m.push_back(new MyMap);
	}
	
	for(i=0; i<R; i++) {
		r.push_back(new MyReduce);
	}
	MapReduce<int,int,int> mr(&m,&r);
	mr.setInput( a,10 );
	long outsize;
	std::pair<int,int>*out=mr.run_and_wait(&outsize);
	
	/*for(i=0; i<M; i++) {
		delete m.at(i);
	}*/
	
	std::cout << "\n\n\n\n";
	for(i=0; i<outsize; i++) {
		std::cout << out[i].first << " : " << out[i].second << "\n";
	}
	
	
	for(i=0; i<R; i++) {
		delete r.at(i);
	}
	std::cout << "the end\n";
	delete[] out;
	
	return 0;
}
