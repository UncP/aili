#! /bin/sh

make thirty_party
                                                  # tree_name thread_num thread_key_num
make one_test "DFLAGS+=-DTest -DDebug -DAllocator" && ./one_test  $1        $2          $3

                                                                            # f_name  n_size  b_size  t_num  q_num  k_num
# make "DFLAGS+=-DTest -DAllocator -DPrefix" palm_tree_test && ./palm_tree_test  1       4096    4096    $2     8      $3
# make "DFLAGS+=-DTest -DAllocator -DPrefix" palm_tree_test && ./palm_tree_test  1       65536   65536   $2     8      $3
# make "DFLAGS+=-DTest -DAllocator -DPrefix -DBStar" palm_tree_test && ./palm_tree_test  1      4096    4096     2      8     1000000

                                                                    # f_name  n_size  t_num  k_num
# make "DFLAGS+=-DTest -DAllocator" blink_tree_test && ./blink_tree_test  1      4096    $2     $3
                                                            # f_name  t_num  k_num
# make mass_tree_test "DFLAGS+=-DAllocator" && ./mass_tree_test  1       $2     $3
                                            # f_name  t_num  k_num
# make art_test "DFLAGS+=-DDebug" && ./art_test  1       $2     $3
