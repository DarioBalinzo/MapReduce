//#define DEBUG 1
#define TIMING 1
#include<time.h>
#include<sys/time.h>
#include "skeleton.hpp"
#include "word.hpp"
#include "string.hpp"
#include <fstream> //reading file
#include <ctype.h> //lower case string conversion


long R;
long M;




class MyMap : public Map<line,word,long> {

		long one=1;
		word w;
		
		void map( line l) {
			int i=0;
			int j;
			/*//lower case the input
			for(i = 0; i<LINE; i++){
				if (l.buffer[i]== '\0')
					break;
  				l.buffer[i] = tolower(l.buffer[i]);
  			}*/
  			
  			for(i = 0; i<LINE; i++){
  				if(l.buffer[i]=='\0') {
  					break;
  				}
  				w.buffer[0]='\0'; //delete previus string
  				j=0;
  				while (i<LINE && j<WORD) {
  					l.buffer[i] = tolower(l.buffer[i]);
  					if (!islower(l.buffer[i]))
  						break;
  					w.buffer[j]=l.buffer[i]; //copy the word
  					j++; i++;
  				}
  				
  				if (j<WORD) 
  					w.buffer[j]='\0';
  				else
  					w.buffer[j-1]='\0';
  				
  				//if the string is not empty, emit
  				if (w.buffer[0]!='\0') {
  					#ifdef DEBUG
  					std::cout << "emitting: ";
  					w.print();
  					#endif
  					emit(&w,&one);
  				}
  				
  				if(i<LINE && l.buffer[i]=='\0') {
  					break;
  				}
  				
  			}	
		}
};


class MyReduce : public Reduce<word,long> {


		long reduce(long a, long b) {
			return a+b;
		}
		
		
};





int main(int argc, char* argv[]) {

	if (argc < 4) {
		std::cerr << "usage: " << argv[0] << " #mapper #input path_input\n";
		return -1;
	}
	
	
	M=atoi(argv[1]);
	R=atoi(argv[2]);

	/*allocating input array*/
	line *input;
	line buffer;
	long i;
	
	
	std::ifstream myfile;
	std::ifstream myfile2;
	myfile.open (argv[3]);
	long lines=-1;
	std::cout << "started\n";
	while (myfile.good()) {
		myfile.getline(buffer.getAll(), LINE);
		lines++;
	}
	std::cout <<lines <<"\n";
	
	myfile.close();
	
	myfile2.open (argv[3]);
	input=new line[lines];
	
	/*populating the input array*/
	for(i=0; i<lines; i++)
		myfile2.getline(input[i].buffer, LINE);
	
	#ifdef DEBUG
	/*printing input*/
	for (i=0; i<lines; i++)
		input[i].print();
	#endif
	
	/*MapReduce*/
	//std::cout << "MAPREDUCE\n";
	
	

	
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
	
	
	
	//std::cout << "END MAPREDUCE\n";
	for(i=0; i<R; i++) {
		delete r.at(i);
	}
	
	for(i=0; i<M; i++) {
		delete m.at(i);
	}
	#ifdef DEBUG
	std::cout <<"\n\n\n";
	for (i=0; i<outsize; i++) {
		out[i].first.print();
		std::cout << out[i].second << "\n\n";
	
	}
	#endif
	
	
	
	delete[] out;
	delete[] input;
	
	return 0;
	
  	

}

