for t in $( cat ../rx.tags  ); do
  echo; echo $t at $( date )
  # echo git checkout $t -b me_${t}
  # git checkout $t -b me_${t}
  git checkout me_${t} > ../bld.${t} 2> ../e.${t}
  git branch -a | grep "^\*" > ../bld.${t}
  tail -1 ../bld.${t}

  mv build_tools build_tools.orig
  ln -s build_tools.6221 build_tools

  echo STEP make clean >> ../bld.${t}
  make clean >> ../bld.${t} 2>& 1
  echo STEP make all >> ../bld.${t}
  DISABLE_WARNING_AS_ERROR=1 DEBUG_LEVEL=0 make V=1 VERBOSE=1 -j16 static_lib db_bench >> ../bld.${t} 2> ../e.${t}
  ldd db_bench
  mv db_bench ../db_bench.${t}
  tail -2 ../bld.${t}

  rm build_tools
  mv build_tools.orig build_tools
done
