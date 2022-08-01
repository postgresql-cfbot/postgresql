#ifndef _COMPRESS_GZIP_H_
#define _COMPRESS_GZIP_H_

#include "compress_io.h"

extern void InitCompressorGzip(CompressorState *cs, int compressionLevel);
extern void InitCompressGzip(CompressFileHandle * CFH, int compressionLevel);

#endif							/* _COMPRESS_GZIP_H_ */
