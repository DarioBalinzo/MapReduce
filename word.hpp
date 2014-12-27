#define WORD 20
class word {

public:
	char buffer[WORD];

	void print() {
		std::cout << buffer << "\n";
	}
	
	void print() const{
		std::cout << buffer << "\n";
	}

	void set(int i, char a) {
		buffer[i]=a;
	}
	char get(int i) {
		return buffer[i];
	}
	char* getAll() {
		return buffer;
	}
	
	const char* getAll() const{
		return (const char*) buffer;
	}
	
	char get(int i) const {
		return (const char) buffer[i];
	}
	
	
	
	bool operator==(const word &other) const { 
  		const char* bf=other.getAll();
  		unsigned i;
  		for(i=0; i<WORD; i++) {
  			if ( buffer[i] == '\0' && bf[i]== '\0' )
  				return true;
  			if ( buffer[i] != bf[i]  )
  				return false;
  		}
  		return true;
  }
  
  bool operator>(const word &l) const {
		unsigned i;
		const char* bf=l.getAll();
		for (i=0; i<WORD-1; i++) {
			if(bf[i] == '\0' || buffer[i]== '\0')
				break;
			if ( bf[i]!=buffer[i] )
				break;
		}
		return buffer[i] > bf[i];
	} 
  
  bool operator<(const word &l) const {
		unsigned i;
		const char* bf=l.getAll();
		for (i=0; i<WORD-1; i++) {
			if(bf[i] == '\0' || buffer[i]== '\0')
				break;
			if ( bf[i]!=buffer[i] )
				break;
		}
		return buffer[i] < bf[i];
	} 
};

namespace std {

	template <>
	struct hash<word> {
	    std::size_t operator()(const word& k) const {
			
			int hash=7;
			for (int i=0; i < 5; i++) {
				if(k.get(i) == '\0')
					break;
    			hash = hash*31+k.get(i);
			}

			return hash;
    }
  };

}

