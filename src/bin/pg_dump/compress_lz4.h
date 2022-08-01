#ifndef _COMPRESS_LZ4_H_
#define _COMPRESS_LZ4_H_

#include "compress_io.h"

extern void InitCompressorLZ4(CompressorState *cs, int compressionLevel);
extern void InitCompressLZ4(CompressFileHandle * CFH, int compressionLevel);

#endif							/* _COMPRESS_LZ4_H_ */
