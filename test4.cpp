/*
*	WordCount
*
*	author: Dario Balinzo
*/

//#define DEBUG 1
#define TIMING 1


//need only for timing pourpose
#include<time.h> 
#include<sys/time.h>


#include "mapreduce.hpp"


#include "word.hpp"
#include "string.hpp"
#include <fstream> //reading file
#include <ctype.h> //lower case string conversion


/*Number of worker in map phase and in the reduce phase*/
long R;
long M;





/*
*	Defining Map class
*	
*	The metod map takes in input a line, custom type string (a buffer of 160 char)
*	and emit a couple <word,long>, where word is a buffer of 15 char
*/
class MyMap : public Map<line,word,long> {
		long one=1;
		word w;
		
		void map( line l) {
			int i=0;
			int j;
			char nextChar;

  			/*reading a line of text, emit every word founded*/
  			for(i = 0; i<LINE; i++){
  				if(l.buffer[i]=='\0') {
  					break;
  				}
  				w.buffer[0]='\0'; //delete previus string
  				j=0;
  				//while there is a word to read
  				while (i<LINE && j<WORD) {
  					nextChar = tolower(l.buffer[i]);
  					if (!islower(nextChar))
  						break;//the word is end
  					w.buffer[j]=l.buffer[i]; //copy the word
  					j++; i++;
  				}
  				
  				//set the end of the word correctly
  				if (j<WORD) 
  					w.buffer[j]='\0';
  				else
  					w.buffer[j-1]='\0';
  				
  				//if the string is not empty, emit
  				if (w.buffer[0]!='\0') {
  					
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
	//std::cout << "started\n";
	while (myfile.good()) {
		myfile.getline(buffer.getAll(), LINE);
		lines++;
	}
	//std::cout <<lines <<"\n";
	
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

