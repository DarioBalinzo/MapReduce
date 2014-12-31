MapReduce
=========

A simple C++ template library, targeting symmetric multicore, enabling fast data processing.


3 Step Tutorial
===============

1. implement the map method (member field will be replicate on each map worker, so use it for  object reuse)
pseudocode:


```
class MyMap : public Map<line,word,long> {
		long one=1;
		word w;
		void map( line l) {	
  				foreach word w in string l	
  					emit(&w,&one);		
		}
};
```


2.implement a reduce method (will be applied only to value belonging to the same key, the type of output must be equal of value type)


```
class MyReduce : public Reduce<word,long> {


		long reduce(long a, long b) {
			return a+b;
		}
		
		
};
```




3.run! (you have only to create two vector with Maps and Reduces, in these way you are also defining the parallel degree used)


```
	std::vector<Map<line,word,long>*> m;
	std::vector<Reduce<word,long>*> r;
	for(i=0; i<M; i++) {
		m.push_back(new MyMap);
	}
	
	for(i=0; i<R; i++) {
		r.push_back(new MyReduce);
	}
	MapReduce<line,word,long> mr(&m,&r);
	mr.setInput( input ,lines );
	long outsize;
	std::pair<word,long>*out=mr.run_and_wait(&outsize);
```
