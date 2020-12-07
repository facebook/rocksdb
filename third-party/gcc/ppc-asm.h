/* PowerPC asm definitions for GNU C.

Copyright (C) 2002-2020 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

Under Section 7 of GPL version 3, you are granted additional
permissions described in the GCC Runtime Library Exception, version
3.1, as published by the Free Software Foundation.

You should have received a copy of the GNU General Public License and
a copy of the GCC Runtime Library Exception along with this program;
see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
<http://www.gnu.org/licenses/>.  */

/* Under winnt, 1) gas supports the following as names and 2) in particular
   defining "toc" breaks the FUNC_START macro as ".toc" becomes ".2" */

#define r0	0
#define sp	1
#define toc	2
#define r3	3
#define r4	4
#define r5	5
#define r6	6
#define r7	7
#define r8	8
#define r9	9
#define r10	10
#define r11	11
#define r12	12
#define r13	13
#define r14	14
#define r15	15
#define r16	16
#define r17	17
#define r18	18
#define r19     19
#define r20	20
#define r21	21
#define r22	22
#define r23	23
#define r24	24
#define r25	25
#define r26	26
#define r27	27
#define r28	28
#define r29	29
#define r30	30
#define r31	31

#define cr0	0
#define cr1	1
#define cr2	2
#define cr3	3
#define cr4	4
#define cr5	5
#define cr6	6
#define cr7	7

#define f0	0
#define f1	1
#define f2	2
#define f3	3
#define f4	4
#define f5	5
#define f6	6
#define f7	7
#define f8	8
#define f9	9
#define f10	10
#define f11	11
#define f12	12
#define f13	13
#define f14	14
#define f15	15
#define f16	16
#define f17	17
#define f18	18
#define f19	19
#define f20	20
#define f21	21
#define f22	22
#define f23	23
#define f24	24
#define f25	25
#define f26	26
#define f27	27
#define f28	28
#define f29	29
#define f30	30
#define f31	31

#ifdef __VSX__
#define f32	32
#define f33	33
#define f34	34
#define f35	35
#define f36	36
#define f37	37
#define f38	38
#define f39	39
#define f40	40
#define f41	41
#define f42	42
#define f43	43
#define f44	44
#define f45	45
#define f46	46
#define f47	47
#define f48	48
#define f49	49
#define f50	50
#define f51	51
#define f52	52
#define f53	53
#define f54	54
#define f55	55
#define f56	56
#define f57	57
#define f58	58
#define f59	59
#define f60	60
#define f61	61
#define f62	62
#define f63	63
#endif

#ifdef __ALTIVEC__
#define v0	0
#define v1	1
#define v2	2
#define v3	3
#define v4	4
#define v5	5
#define v6	6
#define v7	7
#define v8	8
#define v9	9
#define v10	10
#define v11	11
#define v12	12
#define v13	13
#define v14	14
#define v15	15
#define v16	16
#define v17	17
#define v18	18
#define v19	19
#define v20	20
#define v21	21
#define v22	22
#define v23	23
#define v24	24
#define v25	25
#define v26	26
#define v27	27
#define v28	28
#define v29	29
#define v30	30
#define v31	31
#endif

#ifdef __VSX__
#define vs0	0
#define vs1	1
#define vs2	2
#define vs3	3
#define vs4	4
#define vs5	5
#define vs6	6
#define vs7	7
#define vs8	8
#define vs9	9
#define vs10	10
#define vs11	11
#define vs12	12
#define vs13	13
#define vs14	14
#define vs15	15
#define vs16	16
#define vs17	17
#define vs18	18
#define vs19	19
#define vs20	20
#define vs21	21
#define vs22	22
#define vs23	23
#define vs24	24
#define vs25	25
#define vs26	26
#define vs27	27
#define vs28	28
#define vs29	29
#define vs30	30
#define vs31	31
#define vs32	32
#define vs33	33
#define vs34	34
#define vs35	35
#define vs36	36
#define vs37	37
#define vs38	38
#define vs39	39
#define vs40	40
#define vs41	41
#define vs42	42
#define vs43	43
#define vs44	44
#define vs45	45
#define vs46	46
#define vs47	47
#define vs48	48
#define vs49	49
#define vs50	50
#define vs51	51
#define vs52	52
#define vs53	53
#define vs54	54
#define vs55	55
#define vs56	56
#define vs57	57
#define vs58	58
#define vs59	59
#define vs60	60
#define vs61	61
#define vs62	62
#define vs63	63
#endif

/*
 * Macros to glue together two tokens.
 */

#ifdef __STDC__
#define XGLUE(a,b) a##b
#else
#define XGLUE(a,b) a/**/b
#endif

#define GLUE(a,b) XGLUE(a,b)

/*
 * Macros to begin and end a function written in assembler.  If -mcall-aixdesc
 * or -mcall-nt, create a function descriptor with the given name, and create
 * the real function with one or two leading periods respectively.
 */

#if defined(__powerpc64__) && _CALL_ELF == 2

/* Defining "toc" above breaks @toc in assembler code.  */
#undef toc

#define FUNC_NAME(name) GLUE(__USER_LABEL_PREFIX__,name)
#ifdef __PCREL__
#define JUMP_TARGET(name) GLUE(FUNC_NAME(name),@notoc)
#define FUNC_START(name) \
	.type FUNC_NAME(name),@function; \
	.globl FUNC_NAME(name); \
FUNC_NAME(name): \
	.localentry FUNC_NAME(name),1
#else
#define JUMP_TARGET(name) FUNC_NAME(name)
#define FUNC_START(name) \
	.type FUNC_NAME(name),@function; \
	.globl FUNC_NAME(name); \
FUNC_NAME(name): \
0:	addis 2,12,(.TOC.-0b)@ha; \
	addi 2,2,(.TOC.-0b)@l; \
	.localentry FUNC_NAME(name),.-FUNC_NAME(name)
#endif /* !__PCREL__ */

#define HIDDEN_FUNC(name) \
  FUNC_START(name) \
  .hidden FUNC_NAME(name);

#define FUNC_END(name) \
	.size FUNC_NAME(name),.-FUNC_NAME(name)

#elif defined (__powerpc64__)

#define FUNC_NAME(name) GLUE(.,name)
#define JUMP_TARGET(name) FUNC_NAME(name)
#define FUNC_START(name) \
	.section ".opd","aw"; \
name: \
	.quad GLUE(.,name); \
	.quad .TOC.@tocbase; \
	.quad 0; \
	.previous; \
	.type GLUE(.,name),@function; \
	.globl name; \
	.globl GLUE(.,name); \
GLUE(.,name):

#define HIDDEN_FUNC(name) \
  FUNC_START(name) \
  .hidden name;	\
  .hidden GLUE(.,name);

#define FUNC_END(name) \
GLUE(.L,name): \
	.size GLUE(.,name),GLUE(.L,name)-GLUE(.,name)

#elif defined(_CALL_AIXDESC)

#ifdef _RELOCATABLE
#define DESC_SECTION ".got2"
#else
#define DESC_SECTION ".got1"
#endif

#define FUNC_NAME(name) GLUE(.,name)
#define JUMP_TARGET(name) FUNC_NAME(name)
#define FUNC_START(name) \
	.section DESC_SECTION,"aw"; \
name: \
	.long GLUE(.,name); \
	.long _GLOBAL_OFFSET_TABLE_; \
	.long 0; \
	.previous; \
	.type GLUE(.,name),@function; \
	.globl name; \
	.globl GLUE(.,name); \
GLUE(.,name):

#define HIDDEN_FUNC(name) \
  FUNC_START(name) \
  .hidden name; \
  .hidden GLUE(.,name);

#define FUNC_END(name) \
GLUE(.L,name): \
	.size GLUE(.,name),GLUE(.L,name)-GLUE(.,name)

#else

#define FUNC_NAME(name) GLUE(__USER_LABEL_PREFIX__,name)
#if defined __PIC__ || defined __pic__
#define JUMP_TARGET(name) FUNC_NAME(name@plt)
#else
#define JUMP_TARGET(name) FUNC_NAME(name)
#endif
#define FUNC_START(name) \
	.type FUNC_NAME(name),@function; \
	.globl FUNC_NAME(name); \
FUNC_NAME(name):

#define HIDDEN_FUNC(name) \
  FUNC_START(name) \
  .hidden FUNC_NAME(name);

#define FUNC_END(name) \
GLUE(.L,name): \
	.size FUNC_NAME(name),GLUE(.L,name)-FUNC_NAME(name)
#endif

#ifdef IN_GCC
/* For HAVE_GAS_CFI_DIRECTIVE.  */
#include "auto-host.h"

#ifdef HAVE_GAS_CFI_DIRECTIVE
# define CFI_STARTPROC			.cfi_startproc
# define CFI_ENDPROC			.cfi_endproc
# define CFI_OFFSET(reg, off)		.cfi_offset reg, off
# define CFI_DEF_CFA_REGISTER(reg)	.cfi_def_cfa_register reg
# define CFI_RESTORE(reg)		.cfi_restore reg
#else
# define CFI_STARTPROC
# define CFI_ENDPROC
# define CFI_OFFSET(reg, off)
# define CFI_DEF_CFA_REGISTER(reg)
# define CFI_RESTORE(reg)
#endif
#endif

#if defined __linux__ && !defined __powerpc64__
	.section .note.GNU-stack
	.previous
#endif
