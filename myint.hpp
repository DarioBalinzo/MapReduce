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
