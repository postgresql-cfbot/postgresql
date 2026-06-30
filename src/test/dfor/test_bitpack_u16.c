/*
 * test_bitpack.c
 */

#include <stdio.h>
#include <string.h>

#include "lib/bitpack_u16.h"
#include "lib/vect_u16.h"
#include "libtap/tap.h"

int
main(void)
{
	plan(195);
	printf("========================================\n");
	printf("Test MASK AND WIDTH CALCULATION\n");
	{
		cmp_ok(1, "==", (uint16_t)width_u16_from_val(0x00),
			   "Width of 00 is equal to 1 bit.");
		cmp_ok(2, "==", (uint16_t)width_u16_from_val(0x02),
			   "Width of 02 is equal to 2 bits.");
		cmp_ok(2, "==", (uint16_t)width_u16_from_val(0x02),
			   "Width of 02 is equal to 2 bits.");
		cmp_ok(2, "==", (uint16_t)width_u16_from_val(0x03),
			   "Width of 03 is equal to 2 bits.");
		cmp_ok(3, "==", (uint16_t)width_u16_from_val(0x04),
			   "Width of 04 is equal to 3 bits.");
		cmp_ok(3, "==", (uint16_t)width_u16_from_val(0x05),
			   "Width of 05 is equal to 3 bits.");
		cmp_ok(4, "==", (uint16_t)width_u16_from_val(0x08),
			   "Width of 08 is equal to 4 bits.");
		cmp_ok(4, "==", (uint16_t)width_u16_from_val(0x0A),
			   "Width of 10 is equal to 4 bits.");
		cmp_ok(4, "==", (uint16_t)width_u16_from_val(0x0F),
			   "Width of 15 is equal to 4 bits.");
		cmp_ok(5, "==", (uint16_t)width_u16_from_val(0x10),
			   "Width of 16 is equal to 5 bits.");
		cmp_ok(5, "==", (uint16_t)width_u16_from_val(0x1F),
			   "Width of 31 is equal to 5 bits.");
		cmp_ok(6, "==", (uint16_t)width_u16_from_val(0x20),
			   "Width of 32 is equal to 6 bits.");
		cmp_ok(7, "==", (uint16_t)width_u16_from_val(0x40),
			   "Width of 64 is equal to 7 bits.");
		cmp_ok(8, "==", (uint16_t)width_u16_from_val(0x80),
			   "Width of 128 is equal to 8 bits.");
		cmp_ok(13, "==", (uint16_t)width_u16_from_val(0x1000),
			   "Width of 0x01000 is equal to 13 bits.");
		cmp_ok(16, "==", (uint16_t)width_u16_from_val(0x8ABC),
			   "Width of 0x08ABC is equal to 15 bits.");

		cmp_ok(0x1, "==", (uint16_t)width_u16_to_mask(1),
			   "Mask from width 1 is 00000001(bin).");
		cmp_ok(0x3, "==", (uint16_t)width_u16_to_mask(2),
			   "Mask from width 2 is 00000011(bin).");
		cmp_ok(0x7, "==", (uint16_t)width_u16_to_mask(3),
			   "Mask from width 3 is 00000111(bin).");
		cmp_ok(0x3FF, "==", (uint16_t)width_u16_to_mask(10),
			   "Mask from width 10 is 0x3FF(bin).");
		cmp_ok(0x7FFF, "==", (uint16_t)width_u16_to_mask(15),
			   "Mask from width 15 is 0x7FFF(bin).");
		cmp_ok(0xFFFF, "==", (uint16_t)width_u16_to_mask(16),
			   "Mask from width 16 is 0xFFFF(bin).");
	}
	printf("Test MASK AND WIDTH CALCULATION PASSED\n");
	printf("========================================\n\n");

	printf("========================================\n");
	printf("Test BITPACK PACKING\n");
	{
#define SIZEOFPACK 60U
		uint8_t pack[SIZEOFPACK];
		size_t caret = 0;
		memset(pack, 0, SIZEOFPACK);
		/*
		 * Since we implemented the nulifying of bits according to a mask (see
		 * the bitpack function), we can use even a pack not prepared in advance
		 * and comprising garbage. But we want to check value of each byte of
		 * the pack in this test and we simplify this task by using a zeroed
		 * pack.
		 */

		for (int j = 0; j < 8; j++) {
			caret = bitpack_u16_pack(pack, caret, 1, 1);
			caret = bitpack_u16_pack(pack, caret, 0, 1);
		}
		cmp_ok(16, "==", caret, "Caret = 16.");
		cmp_ok(0x55, "==", pack[0], "Saved bit-by-bit: first byte is 0x55.");
		cmp_ok(0x55, "==", pack[1], "Saved bit-by-bit: second byte is 0x55.");

		for (int j = 0; j < 8; j++) {
			caret = bitpack_u16_pack(pack, caret, 0x02 /* 10(bin) */, 2);
		}

		cmp_ok(32, "==", caret, "Caret = 32.");
		cmp_ok(0xAA, "==", pack[2],
			   "Saved with two-bit width: first byte is 0xAA.");
		cmp_ok(0xAA, "==", pack[3],
			   "Saved with two-bit width: second byte is 0xAA.");

		for (int j = 0; j < 8; j++) {
			caret = bitpack_u16_pack(pack, caret, 0x05 /* 101(bin) */, 3);
			caret = bitpack_u16_pack(pack, caret, 0x02 /* 010(bin) */, 3);
		}

		cmp_ok(80, "==", caret, "Caret = 80.");
		cmp_ok(0x55, "==", pack[4],
			   "Saved with three-bit width: first byte is 0x55.");
		cmp_ok(0x55, "==", pack[5],
			   "Saved with three-bit width: second byte is 0x55.");
		cmp_ok(0x55, "==", pack[6],
			   "Saved with three-bit width: second byte is 0x55.");
		cmp_ok(0x55, "==", pack[7],
			   "Saved with three-bit width: first byte is 0x55.");
		cmp_ok(0x55, "==", pack[8],
			   "Saved with three-bit width: second byte is 0x55.");
		cmp_ok(0x55, "==", pack[9],
			   "Saved with three-bit width: second byte is 0x55.");

		caret = bitpack_u16_pack(pack, caret, 0x0B, 4);
		caret = bitpack_u16_pack(pack, caret, 0x0C, 4);
		caret = bitpack_u16_pack(pack, caret, 0x0D, 4);
		caret = bitpack_u16_pack(pack, caret, 0x0E, 4);

		cmp_ok(96, "==", caret, "Caret = 96.");
		cmp_ok(0xCB, "==", pack[10],
			   "Saved with four-bit width: first byte is 0xCB.");
		cmp_ok(0xED, "==", pack[11],
			   "Saved with four-bit width: second byte is 0xED.");

		for (int j = 0; j < 8; j++) {
			caret = bitpack_u16_pack(pack, caret, 0x07 /* 00111b */, 5);
		}

		cmp_ok(136, "==", caret, "Caret = 136.");
		cmp_ok(0xE7, "==", pack[12],
			   "Saved with five-bit width: first byte is 0xE7.");
		cmp_ok(0x9C, "==", pack[13],
			   "Saved with five-bit width: second byte is 0x9C.");
		cmp_ok(0x73, "==", pack[14],
			   "Saved with five-bit width: third byte is 0x73.");
		cmp_ok(0xCE, "==", pack[15],
			   "Saved with five-bit width: fourth byte is 0xCE.");
		cmp_ok(0x39, "==", pack[16],
			   "Saved with five-bit width: fifth byte is 0x39.");

		for (int j = 0; j < 4; j++)
			caret = bitpack_u16_pack(pack, caret, 0x07 /* 000111b */, 6);

		cmp_ok(160, "==", caret, "Caret = 160.");
		cmp_ok(0xC7, "==", pack[17],
			   "Saved with six-bit width: first byte is 0xC7.");
		cmp_ok(0x71, "==", pack[18],
			   "Saved with six-bit width: second byte is 0x71.");
		cmp_ok(0x1C, "==", pack[19],
			   "Saved with six-bit width: third byte is 0x1C.");

		for (int j = 0; j < 8; j++)
			caret = bitpack_u16_pack(pack, caret, 0x57 /* 1010111b */, 7);

		cmp_ok(216, "==", caret, "Caret = 216.");
		cmp_ok(0xD7, "==", pack[20],
			   "Saved with seven-bit width: byte1 is 0xD7.");
		cmp_ok(0xEB, "==", pack[21],
			   "Saved with seven-bit width: byte2 is 0xEB.");
		cmp_ok(0xF5, "==", pack[22],
			   "Saved with seven-bit width: byte3 is 0xF5.");
		cmp_ok(0x7A, "==", pack[23],
			   "Saved with seven-bit width: byte4 is 0x7A.");
		cmp_ok(0xBD, "==", pack[24],
			   "Saved with seven-bit width: byte5 is 0xBD.");
		cmp_ok(0x5E, "==", pack[25],
			   "Saved with seven-bit width: byte6 is 0x5E.");
		cmp_ok(0xAF, "==", pack[26],
			   "Saved with seven-bit width: byte7 is 0xAF.");

		caret = bitpack_u16_pack(pack, caret, 0xBA, 8);
		caret = bitpack_u16_pack(pack, caret, 0xDC, 8);
		caret = bitpack_u16_pack(pack, caret, 0xFE, 8);

		cmp_ok(240, "==", caret, "Caret = 240.");
		cmp_ok(0xBA, "==", pack[27],
			   "Saved with eight-bit width: byte1 is 0xBA.");
		cmp_ok(0xDC, "==", pack[28],
			   "Saved with eight-bit width: byte2 is 0xDC.");
		cmp_ok(0xFE, "==", pack[29],
			   "Saved with eight-bit width: byte3 is 0xFE.");

		caret = bitpack_u16_pack(
			pack, caret, 0x00,
			4); /* move to 4 bit for fun and again continue with 8-bit width */
		caret = bitpack_u16_pack(pack, caret, 0x32, 8);
		caret = bitpack_u16_pack(pack, caret, 0x54, 8);
		caret = bitpack_u16_pack(pack, caret, 0x76, 8);
		caret = bitpack_u16_pack(pack, caret, 0x98, 8);

		cmp_ok(276, "==", caret, "Caret = 276.");
		cmp_ok(0x20, "==", pack[30],
			   "Saved with eight-bit width but shifted by 4: is 0x20.");
		cmp_ok(0x43, "==", pack[31],
			   "Saved with eight-bit width but shifted by 4: is 0x43.");
		cmp_ok(0x65, "==", pack[32],
			   "Saved with eight-bit width but shifted by 4: is 0x65.");
		cmp_ok(0x87, "==", pack[33],
			   "Saved with eight-bit width but shifted by 4: is 0x87.");
		cmp_ok(0x09, "==", pack[34],
			   "Saved with eight-bit width but shifted by 4: is 0x09.");

		caret = bitpack_u16_pack(
			pack, caret, 0x00,
			4); /* move to 4 bit for fun and again continue with 8-bit width */
		cmp_ok(280, "==", caret, "Caret = 280.");
		cmp_ok(0x09, "==", pack[34],
			   "Added padding 0x0, width=4. Byte in pack is still 0x09.");

		for (int j = 0; j < 3; j++)
			caret = bitpack_u16_pack(pack, caret, 0x1671 /* 1011001110001b */,
									 13);

		cmp_ok(319, "==", caret, "Caret = 319.");
		cmp_ok(0x71, "==", pack[35], "Saved with thirteen-bit width: is 0x71.");
		cmp_ok(0x36, "==", pack[36], "Saved with thirteen-bit width: is 0x36.");
		cmp_ok(0xCE, "==", pack[37], "Saved with thirteen-bit width: is 0xCE.");
		cmp_ok(0xC6, "==", pack[38], "Saved with thirteen-bit width: is 0xC6.");
		cmp_ok(0x59, "==", pack[39], "Saved with thirteen-bit width: is 0x59.");

		caret = bitpack_u16_pack(pack, caret, 0x1 /* PADDING */, 1);
		cmp_ok(320, "==", caret, "Caret = 320.");
		cmp_ok(0xD9, "==", pack[39],
			   "After padding with 0x01, w=1: 0x59 -> 0xD9.");

		for (int j = 0; j < 5; j++)
			caret = bitpack_u16_pack(pack, caret, 0xCDEF, 16);

		cmp_ok(400, "==", caret, "Caret = 400.");
		for (int i = 40; i < 50;) {
			cmp_ok(0xEF, "==", pack[i++], "Packed with width=16. 0xEF.");
			cmp_ok(0xCD, "==", pack[i++], "Packed with width=16. 0xC.");
		}

		caret = bitpack_u16_pack(pack, caret,
								 0x0 /* PADDING in order to shift by 1 bit*/,
								 1);
		cmp_ok(401, "==", caret, "Caret = 401.");

		for (int j = 0; j < 3; j++)
			caret = bitpack_u16_pack(pack, caret, 0x5555, 16);

		cmp_ok(449, "==", caret, "Caret = 401.");
		for (int i = 50; i < 56;)
			cmp_ok(0xAA, "==", pack[i++],
				   "16-bit value saved with shift by 1 bit 0x55->0xAA.");

		cmp_ok(0x0, "==", pack[56], "1 higher bit is alone .");

		printf("Test BITPACK PACKING PASSED\n");
		printf("========================================\n\n");

		printf("========================================\n");
		printf("Test BITPACK UNPACKING\n");

		caret = 0;

		for (int j = 0; j < 8; j++) {
			cmp_ok(0x01, "==", bitpack_u16_unpack(pack, &caret, 1));
			cmp_ok(0x00, "==", bitpack_u16_unpack(pack, &caret, 1));
		}
		cmp_ok(caret, "==", 16);

		for (int j = 0; j < 8; j++)
			cmp_ok(0x02, "==", bitpack_u16_unpack(pack, &caret, 2));

		cmp_ok(32, "==", caret, "Caret = 32.");

		for (int j = 0; j < 8; j++) {
			cmp_ok(0x05, "==", bitpack_u16_unpack(pack, &caret, 3));
			cmp_ok(0x02, "==", bitpack_u16_unpack(pack, &caret, 3));
		}
		cmp_ok(80, "==", caret, "Caret = 80.");

		cmp_ok(0x0B, "==", bitpack_u16_unpack(pack, &caret, 4));
		cmp_ok(0x0C, "==", bitpack_u16_unpack(pack, &caret, 4));
		cmp_ok(0x0D, "==", bitpack_u16_unpack(pack, &caret, 4));
		cmp_ok(0x0E, "==", bitpack_u16_unpack(pack, &caret, 4));

		cmp_ok(96, "==", caret, "Caret = 96.");

		for (int j = 0; j < 8; j++)
			cmp_ok(0x07, "==", bitpack_u16_unpack(pack, &caret, 5),
				   "width=5, val=00111b");

		cmp_ok(136, "==", caret, "Caret = 136.");

		for (int j = 0; j < 4; j++)
			cmp_ok(0x07, "==", bitpack_u16_unpack(pack, &caret, 6),
				   "width=6, val=000111b");

		cmp_ok(160, "==", caret, "Caret = 160.");

		for (int j = 0; j < 8; j++)
			cmp_ok(0x57, "==", bitpack_u16_unpack(pack, &caret, 7),
				   "width=7, val=1010111b (0x57)");

		cmp_ok(216, "==", caret, "Caret = 216.");

		cmp_ok(0xBA, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0xBA");
		cmp_ok(0xDC, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0xDC");
		cmp_ok(0xFE, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0xFE");

		cmp_ok(240, "==", caret, "Caret = 240.");

		cmp_ok(0x0, "==", bitpack_u16_unpack(pack, &caret, 4),
			   "width=4, val=0x0, for fun");

		cmp_ok(0x32, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0x32");
		cmp_ok(0x54, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0x54");
		cmp_ok(0x76, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0x76");
		cmp_ok(0x98, "==", bitpack_u16_unpack(pack, &caret, 8),
			   "width=8, val=0x98");

		cmp_ok(0x0, "==", bitpack_u16_unpack(pack, &caret, 4),
			   "width=4, val=0x0, for fun padding again");

		for (int j = 0; j < 3; j++)
			cmp_ok(0x1671, "==", bitpack_u16_unpack(pack, &caret, 13),
				   "width=13, val=0x1671 (1011001110001b)");

		cmp_ok(319, "==", caret, "Caret = 319.");

		cmp_ok(0x1, "==", bitpack_u16_unpack(pack, &caret, 1),
			   "width=1, val=0x1");
		cmp_ok(320, "==", caret, "Caret = 320.");

		for (int j = 0; j < 5; j++)
			cmp_ok(0xCDEF, "==", bitpack_u16_unpack(pack, &caret, 16),
				   "18-bit value alligned with bytes in pack.");

		cmp_ok(400, "==", caret, "Caret = 400.");
		cmp_ok(0x0, "==", bitpack_u16_unpack(pack, &caret, 1),
			   "1-bit width value (padding for shift).");
		cmp_ok(401, "==", caret, "Caret = 401.");

		for (int j = 0; j < 3; j++)
			cmp_ok(0x5555, "==", bitpack_u16_unpack(pack, &caret, 16),
				   "16-bit width value shifted by 1 bit.");

		cmp_ok(449, "==", caret, "Caret = 449.");
	}

	done_testing();
}
