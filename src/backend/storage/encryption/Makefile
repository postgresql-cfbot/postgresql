#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/encryption
#
# IDENTIFICATION
#    src/backend/storage/encryption/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/encryption/
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = kmgr.o bufenc.o walenc.o enc_cipher.o

ifeq ($(with_openssl),yes)
OBJS += enc_openssl.o
endif

include $(top_srcdir)/src/backend/common.mk
