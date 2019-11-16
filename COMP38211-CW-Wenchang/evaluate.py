# read each line in output file as a list
def read_hadoop_output():
    # read each line in output file as a list
    with open("output/part-r-00000", "r") as f:
        all_lines = f.readlines();
        print("tokens number: " + str(len(all_lines)))
    return all_lines


# parse the results from hadoop
# input: all_lines(as a list) from output text file
# output: a dict, key is filename, value is a dict, key is token, value is tfidf  
def parse_lines(all_lines):
    result_dict = {}
    for line in all_lines:
        token_map_list = line.split('\t')
        # print(token_map_list)
        token = token_map_list[0]
        file_list_map = token_map_list[1]
        # remove {, }, ], enter
        file_list_map = file_list_map[1:-3]
        # print("token: " + token)
        # print("file list map: " + file_list_map)
        file_list_pairs = file_list_map.split("], ")
        for file_list_pair in file_list_pairs:
            tfidf_token_dict = {}
            # print(file_list_pair)
            filename_list_list = file_list_pair.split("=")
            filename = filename_list_list[0]
            tfidf_pos_list = filename_list_list[1]
            tfidf_pos_list = tfidf_pos_list[1:-1]
            # print(filename)
            # print(tfidf_pos_list)
            tfidf_list_split = tfidf_pos_list.split(", [")
            pos_list = tfidf_list_split[1]
            tfidf_list_split = tfidf_list_split[0]
            tfidf_list = tfidf_list_split.split(", ")
            # print(tfidf_list)
            tf = tfidf_list[0]
            df = tfidf_list[1]
            idf = tfidf_list[2]
            tfidf = tfidf_list[3]
            pos_list = pos_list.split(", ")
            # print(pos_list)
            # if you want to see parse results
            # print(
            #     "token: " + token + " ,filename: " + filename + "\n" +
            #     "tf: " + tf + ", df: " + df + ", idf: " + idf + ", tfidf: " + tfidf + "\n" +
            #     "position: " + str(pos_list)
            # )
            if filename in result_dict:
                result_dict.get(filename).update({token: tfidf})
            else:
                tfidf_token_dict[token] = tfidf
                result_dict[filename] = tfidf_token_dict
    return result_dict


# output top n tfidf scores tokens for each file
def evaluate_tfidf(input_dict, top_num):
    for filename in input_dict:
        print("Filename: " + filename)
        token_tfidf_pairs = input_dict.get(filename)
        print("number of tokens in the file: " + str(len(token_tfidf_pairs)))
        for i in range(top_num):
            max_key = max(token_tfidf_pairs, key=token_tfidf_pairs.get)
            print(
                "Top " + str(i+1) + " : " + "word: " + max_key + ", " +
                "tfidf: " + token_tfidf_pairs.get(max_key))
            del token_tfidf_pairs[max_key]


all_lines = read_hadoop_output()
tfidf_dict = parse_lines(all_lines)
evaluate_tfidf(tfidf_dict, 5)
