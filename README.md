# Knowledge-base_for_Word_Prediction_using_EMR


#### Created by:
* Tom Levy
* Alon Michaeli

### How to run:

  1. Unzip the submitted files, and create the jar file (alternatively, run Main.java from IDE).

  2. In the same folder, run Main.java such as:
      java -jar WordPredictionEMR.jar <arguments>

  arguments (optional): 
    -la = Run the program with local aggregation

### Program flow:

* Main.java:
        1.1 Main function of the assignment.
		1.2 Responsible for parsing if the run uses local aggregation (args[0]).
		1.3 Initiate the task in Amazon EMR (with the written steps.
		1.4 Finish run.
        
* Step 1:
		
		input: a sequential file (hebrew, 3gram).
		args: isLocalAggregation, input address, output address (in S3)
		Job flow:
			Clean input from invalid Grams.
		    Split the corpus to 2 splits, and for each 3gram, sums up occurrences in each Corpus.
		output: text files, whereas each line:
			<3gram,r0_r1>
		when:
			3gram = legal hebrew tuple from input.
			r_0 = occurrences in corpus 0.
			r_1 = occurrences in corpus 1.

* Step 2:
		
		input: output of Step 1, text file.
			Each line is <3gram, r0_r1>
	
		args: input address, output address (in S3)
		
		Job flow:
			Calculate for every r:
				N_0_r & T_0_r
				N_0_r & T_0_r .
			Reducer sums up for every r the number of 3grams in the given Corpus,
			and the occurrences of those 3grams in the other Corpus.
		
		output: text files, whereas each r:
			<"N_0_r", <N_0_r, T_0_r>> or <"N_0_r", <N_1_r, T_1_r>>
			when:
				"N_0_r" = string represent of the r & Corpus
				<N_0_r, T_0_r> = as defined in the assignment
				<N_1_r, T_1_r> = Same as above

* Step 3:
		input: outputs of Step 1 & Step 2, text files.
			Each line is:
			<3gram, r0_r1>
			or
			<"N_0_r", <N_0_r, T_0_r>> (same for Corpus 1)
		
		args: input addresses, output address (in S3)
		
		Job flow:
			Perform ReduceSideJoin using a secondary sort on the tagged keys.
			The join is on the given input (<3gram, r0_r1>, or <"N_0_r", <N_0_r, T_0_r>>)
				WHERE  r0 = N_0_r (same for Corpus 1)
			Reducer performs the Join as learnt in class (using TaggedKey, crossProduct())
		
		output: text files, whereas for each 3gram:
			<3gram, <N_0_r, T_0_r>
			and
			<3gram, <N_1_r, T_1_r>
			(each 3gram appears in 2 lines)
		
		when:
			N_0_r = as defined in the assignment, and for the 3gram's r in Corpus 0
			T_0_r = as defined in the assignment, and for the 3gram's r in Corpus 0
	
* Step 4:
		input: outputs of Step 34, text files.
			Each line is:
			<3gram, <N_0_r, T_0_r>>
			or
			<3gram, <N_1_r, T_1_r>>
			(each 3gram appears in 2 lines)
		
		args: input addresses, output address (in S3)
		
		Job flow:
			Combine the calculations in the 2 spkits of the corpus,
			and arrange the values for the final step & calculations
		
		output: text files, whereas for each 3gram:
			<3gram, <N', T'>>
		when:
			N' = N_0_r + N_1_r
			T' = T_0_r + T_1_r
		
		Notice: for each gram, r's value is different for each Corpus.
	
	
* Step 5:
	
		input: outputs of Step 4, text files.
			 Each line is:
			 <3gram, <N', T'>>
		
		args: input addresses, output address (in S3)
		
		Job flow:
			 Final step of the assignment.
			 Retrieve N from Step 1, and calculate for each the 3gram his value.
			 create the final output file.
		
		output: text files, whereas for each 3gram:
			 <3gram, probability>
		
		when:
			 probability = T' / (N' * N)
		
  Output is sorted as required in the assignment.

### Reports (Statistics & Analysis):
		
#### Statistics: 
Let us observe the statistics of the program runs, with / without local aggregation, 
and compare the number of key-value pairs that were sent from the mappers to the reducers, and their sizes:

* Without Local aggregation:

	Step 1:
		Map output: 
			number of key-value pairs = 71119513
			size (bytes) = 2329082675

	Step 2:
		Map output:
			number of key-value pairs = 50795526
			size (bytes) = 1990866419

	Step 3:
		Map output:
			number of key-value pairs = 50798885
			size (bytes) = 1787750927

	Step 4:
		Map output:
			number of key-value pairs = 50795526
			size (bytes) = 1680599306

	Step 5:
		Map output:
			number of key-value pairs = 1686118
			size (bytes) = 69392080

* With Local aggregation:
	
	Step 1:		
		Map output:
			number of key-value pairs = 71119513
			size (bytes) = 2329082675
		Combine output, number of key-value pairs = 25397705

	step 2:
		Map output:
			number of key-value pairs = 50795270
			size (bytes) = 1988121729
		Combine output, number of key-value pairs = 0

	step 3:
		Map output:
			number of key-value pairs = 50797500
			size (bytes) = 1784978221
		Combine output, number of key-value pairs = 0

	step 4:
		Map output:
			number of key-value pairs = 50795270
			size (bytes) = 1680601102
		Combine output, number of key-value pairs =37661879

	step 5:
		Map output:
			number of key-value pairs = 1686118
			Map output bytes=69392080
			size (bytes) = 40150620
		Combine output, number of key-value pairs = 0


#### Analysis:
	Let us observe 10 interesting word-pairs and show their top-5 next words:
	1.
		אני רוצה לשאול	1.9810524091760312E-8
		אני רוצה להיות	1.976730709405774E-8
		אני רוצה לדעת	1.9745771990941747E-8
		אני רוצה לראות	1.967435401173209E-8
		אני רוצה לדבר	1.9660303295547112E-8
	2.
		יהודי הונגריה למחנות	1.590579426845849E-8
		יהודי הונגריה לא	1.5750121923429738E-8
		יהודי הונגריה היו	1.5388464051119286E-8
		יהודי הונגריה על	1.5263991712682455E-8
		יהודי הונגריה לאושוויץ	1.5020300123539323E-8
	3.
		יהודי מן העיר	1.675234390853735E-8
		יהודי מן הצבא	1.6429398872401967E-8
		יהודי מן המאה	1.591962201408268E-8
		יהודי מן הדור	1.5559907373835068E-8
		יהודי מן השוק	1.5538995558645995E-8
	4.
		למען קיום התורה	1.7407428828475877E-8
		למען קיום העולם	1.653148006448302E-8
		למען קיום מצוות	1.6416283069321253E-8
		למען קיום העם	1.6370079624550554E-8
		למען קיום האומה	1.5717064076948342E-8
	5.
		למען שמו הגדול	1.9430256939274744E-8
		למען שמו באהבה	1.828077651115815E-8
		למען שמו יתברך	1.7214560585510242E-8
		למען שמו להודיע	1.7085670308829826E-8
		למען שמו ולמען	1.6088339398786702E-8	
	6.
		למצוא את מקום	1.956814643555384E-8
		למצוא את מקומו	1.950166157065766E-8
		למצוא את הקשר	1.935948905931328E-8
		למצוא את הפתרון	1.9329846834699928E-8
		למצוא את מקור	1.919714402802693E-8
	7.
		על כושר הקליטה	1.6079076151875716E-8
		על כושר הייצור	1.5817482911694033E-8
		על כושר גופני	1.5370977700927664E-8
		על כושר הפעולה	1.5152952620075387E-8
		על כושר השיפוט	1.4939259318452686E-8	
	8.
		אני תוהה על	1.8898722370582996E-8
		אני תוהה מה	1.7391498690297746E-8
		אני תוהה אם	1.686652846959469E-8
		אני תוהה מדוע	1.5730289937216868E-8
		אני תוהה האם	1.50072913764634E-8	
	9.
		כי חל שינוי	1.9102436769133833E-8
		כי חל שיפור	1.6689810328857185E-8
		כי חל מפנה	1.6057281422141534E-8
		כי חל גידול	1.5827228997301535E-8
		כי חל איסור	1.5261501775193073E-8	
	10.
		כי חכם גדול	1.874465879956869E-8
		כי חכם הוא	1.754633987352502E-8
		כי חכם זה	1.7171764529425712E-8
		כי חכם אחד	1.6873615988417426E-8
		כי חכם אתה	1.6494356991579854E-8
	
#### Discussion:
  If we look on the 10 different word-pairs and their top-5, we conclude that most of the time the order and probabilities are reasonableble.
  for pair 1, it is reasonableble that "אני רוצה לשאול" would be the most common, since it is a well-used sequense in the hebrew literature (same for the rest 4, but in a decreasing manner).
  We did find some suprises, as pair 2, but it might be that the specific literature discussing the jews of Hungaria, has mostly that tuple.		
  Some of the pairs, such as the 3rd, seem to be logically correct. "יהודי מן העיר" seems to have better probability than "יהודי מן השוק", as it's less specific.
  In the 9th pair, "כי חל שינוי" has quite significant difference from the second one, more than we would expect.
  In addition, it seems that tuple with same "probability" do has similar results (as in the 6th example).
  Overall, we concluded that for those pairs, the first and the fifth do might have a large change in the probabilities, when the "real" order of all 5 might differ from the results (in minor changes). 
		
