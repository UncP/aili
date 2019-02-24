#! /bin/sh

# before you run any test,
# 1. make third_party
                                         # file_num  key_num   key_len
# 2. make generate_data && ./generate_data  4        10000000    10
                                          # data will be in ./data, 4 random data files, 4 sequential data files

if [ "$1" = "palm" ]
then
                                                                            # f_name  n_size  b_size  t_num  q_num  k_num
make "DFLAGS+=-DTest -DAllocator -DPrefix" palm_tree_test && ./palm_tree_test  1       4096    4096    $2     8      $3
# make "DFLAGS+=-DTest -DAllocator -DPrefix" palm_tree_test && ./palm_tree_test  1       65536   65536   $2     8      $3
# make "DFLAGS+=-DTest -DAllocator -DPrefix -DBStar" palm_tree_test && ./palm_tree_test  1      4096    4096     2      8     1000000
elif [ "$1" = "blink" ]
then
                                                                    # f_name  n_size  t_num  k_num
make "DFLAGS+=-DTest -DAllocator" blink_tree_test && ./blink_tree_test  1      4096    $2     $3
elif [ "$1" = "mass" ]
then
                                                            # f_name  t_num  k_num
make mass_tree_test "DFLAGS+=-DAllocator" && ./mass_tree_test  1       $2     $3
elif [ "$1" = "art" ]
then
                                            # f_name  t_num  k_num
make art_test "DFLAGS+=-DDebug" && ./art_test  1       $2     $3
else
  echo "1: palm || blink || mass || art\n2: thread_num\n3: total_key_num\n:("
fi
