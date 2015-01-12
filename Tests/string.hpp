#define LINE 160
class line {

public:
	char buffer[LINE];

	void set(int i, char a) {
		buffer[i]=a;
	}
	char get(int i) {
		return buffer[i];
	}
	char* getAll() {
		return buffer;
	}
	
	void print() {
		std::cout << buffer << "\n";
	}
	
	const char* getAll() const{
		return (const char*) buffer;
	}
	
	char get(int i) const {
		return (const char) buffer[i];
	}
	
	bool operator==(const line &other) const { 
  		const char* bf=other.getAll();
  		unsigned i;
  		for(i=0; i<LINE; i++) {
  			if ( buffer[i] == '\0' && bf[i]== '\0' )
  				return true;
  			if ( buffer[i] != bf[i]  )
  				return false;
  		}
  		return true;
  }
};


namespace std {

	template <>
	struct hash<line> {
	    std::size_t operator()(const line& k) const {
			
			int hash=7;
			for (int i=0; i < LINE; i++) {
    			hash = hash*31+k.get(i);
			}

			return hash;
    }
  };

}

