Reduced cases of illegally using Env::Default() during static destruction by never destroying the internal PosixEnv itself (except for builds checking for memory leaks). (#11538)
