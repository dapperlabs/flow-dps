package zbor

// payloadDictionary is a byte slice that contains the result of running the Zstandard training mode
// on the payloads of the DPS index. This allows zstandard to achieve a better compression ratio, specifically for
// small data.
// See http://facebook.github.io/zstd/#small-data
// See https://github.com/facebook/zstd/blob/master/doc/zstd_compression_format.md#dictionary-format
var payloadDictionary = []byte{
	55, 164, 48, 236, 146, 150, 35, 32, 73, 16, 72, 206, 184, 2, 0, 132, 32, 131, 124, 142, 24, 51, 191, 49, 6, 185, 13, 98, 204, 83, 35, 108, 73, 82, 82, 82, 66, 155, 36, 54, 37, 50, 77, 65, 236, 223, 141, 117, 81, 186, 125, 141, 248, 112, 134, 9, 34, 26, 214, 133, 52, 46, 114, 134, 40, 198, 82, 29, 83, 236, 66, 40, 126, 130, 251, 10, 68, 48, 1, 162, 157, 2, 115, 4, 0, 0, 0, 0, 64, 144, 135, 113, 0, 0, 4, 64, 0, 128, 1, 86, 0, 0, 0, 0, 0, 0, 88, 0, 182, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 8, 0, 0, 156, 0, 0, 0, 0, 0, 0, 120, 0, 0, 0, 192, 3, 252, 108, 0, 0, 0, 20, 184, 11, 0, 16, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 8, 0, 0, 0, 97, 108, 117, 101, 72, 224, 143, 13, 151, 68, 5, 178, 244, 162, 100, 84, 121, 112, 101, 1, 101, 86, 97, 108, 117, 101, 64, 162, 100, 84, 121, 112, 101, 2, 101, 86, 97, 108, 117, 101, 86, 115, 116, 111, 114, 97, 103, 101, 31, 102, 108, 111, 119, 84, 111, 107, 101, 110, 86, 97, 117, 108, 116, 101, 86, 97, 108, 117, 101, 88, 73, 0, 202, 222, 0, 5, 216, 132, 132, 216, 192, 130, 72, 10, 229, 60, 182, 227, 244, 42, 121, 105, 70, 108, 111, 119, 84, 111, 107, 101, 110, 2, 132, 100, 117, 117, 105, 100, 216, 164, 25, 12, 93, 103, 98, 97, 108, 97, 110, 99, 101, 216, 188, 26, 5, 247, 103, 162, 111, 70, 108, 111, 119, 84, 111, 107, 101, 110, 46, 86, 97, 117, 108, 116, 162, 99, 75, 101, 121, 161, 104, 75, 101, 121, 80, 97, 114, 116, 115, 131, 162, 100, 84, 121, 112, 101, 0, 101, 86, 97, 108, 117, 101, 72, 240, 225, 187, 29, 113, 242, 20, 5, 162, 100, 84, 121, 112, 101, 1, 101, 86, 97, 108, 117, 101, 72, 240, 225, 187, 29, 113, 242, 20, 5, 162, 100, 84, 121, 112, 101, 2, 101, 86, 97, 108, 117, 101, 76, 112, 117, 98, 108, 105, 99, 95, 107, 101, 121, 95, 48, 101, 86, 97, 108, 117, 101, 88, 76, 248, 74, 184, 64, 146, 174, 136, 41, 21, 4, 189, 216, 191, 27, 84, 213, 155, 244, 47, 106, 119, 3, 73, 90, 245, 15, 163, 49, 125, 237, 166, 230, 102, 39, 198, 82, 85, 122, 30, 58, 209, 188, 110, 80, 156, 185, 140, 89, 93, 211, 134, 104, 218, 50, 232, 41, 184, 139, 88, 205, 197, 198, 111, 72, 136, 157, 216, 141, 2, 3, 130, 3, 232, 129, 162, 128, 162, 99, 75, 101, 121, 161, 104, 75, 101, 121, 80, 97, 114, 116, 115, 131, 162, 100, 84, 121, 112, 101, 0, 101, 86, 97, 108, 117, 101, 72, 118, 48, 154, 25, 143, 170, 125, 130, 162, 100, 84, 121, 112, 101, 1, 101, 86, 97, 108, 117, 101, 64, 162, 100, 84, 121, 112, 101, 2, 101, 86, 97, 108, 117, 101, 88, 24, 112, 117, 98, 108, 105, 99, 31, 102, 108, 111, 119, 84, 111, 107, 101, 110, 82, 101, 99, 101, 105, 118, 101, 114, 101, 86, 97, 108, 117, 101, 88, 128, 0, 202, 222, 0, 5, 216, 203, 130, 216, 200, 130, 1, 110, 102, 108, 111, 119, 84, 111, 107, 101, 110, 86, 97, 117, 108, 116, 216, 219, 130, 244, 216, 220, 130, 216, 213, 130, 216, 192, 130, 72, 10, 229, 60, 182, 227, 244, 42, 121, 105, 70, 108, 111, 119, 84, 111, 107, 101, 110, 111, 70, 108, 111, 119, 84, 111, 107, 101, 110, 46, 86, 97, 117, 108, 116, 129, 216, 214, 130, 216, 192, 130, 72, 238, 130, 133, 107, 242, 14, 42, 166, 109, 70, 117, 110, 103, 105, 98, 108, 101, 84, 111, 107, 101, 110, 118, 70, 117, 110, 103, 105, 98, 108, 101, 84, 111, 107, 101, 110, 46, 82, 101, 99, 101, 105, 118, 101, 114, 162, 99, 75, 101, 121, 161, 104, 75, 101, 121, 80, 97, 114, 116, 115, 131, 162, 100, 84, 121, 112, 101, 0, 101, 86, 97, 108, 117, 101, 72, 148, 223, 83, 84, 174, 108, 101, 44, 162, 100, 84, 121, 112, 101, 1, 101, 86, 97, 108, 117, 101, 64, 162, 100, 84, 121, 112, 101, 2, 101, 86, 97, 108, 117, 101, 86, 115, 116, 111, 114, 97, 103, 101, 31, 102, 108, 111, 119, 84, 111, 107, 101, 110, 86, 97, 117, 108, 116, 101, 86, 97, 108, 117, 101, 88, 73, 0, 202, 222, 0, 5, 216, 132, 132, 216, 192, 130, 72, 10, 229, 60, 182, 227, 244, 42, 121, 105, 70, 108, 111, 119, 84, 111, 107, 101, 110, 2, 132, 100, 117, 117, 105, 100, 216, 164, 25, 26, 49, 103, 98, 97, 108, 97, 110, 99, 101, 216, 188, 26, 5, 247, 103, 184, 111, 70, 108, 111, 119, 84, 111, 107, 101, 110, 46, 86, 97, 117, 108, 116, 162, 99, 75, 101, 121, 161, 104, 75, 101, 121, 80, 97, 114, 116, 115, 131, 162, 100, 84, 121, 112, 101, 0, 101, 86, 97, 108, 117, 101, 72, 240, 225, 187, 29, 113, 242, 20, 5, 162, 100, 84, 121, 112, 101, 1, 101, 86, 97, 108, 117, 101, 72, 240, 225, 187, 29, 113, 242, 20, 5, 162, 100, 84, 121, 112, 101, 2, 101, 86, 97, 108, 117, 101, 76, 112, 117, 98, 108, 105, 99, 95, 107, 101, 121, 95, 48, 101, 86, 97, 108, 117, 101, 88, 75, 248, 73, 184, 64, 146, 174, 136, 41, 21, 4, 189, 216, 191, 27, 84, 213, 155, 244, 47, 106, 119, 3, 73, 90, 245, 15, 163, 49, 125, 237, 166, 230, 102, 39, 198, 82, 85, 122, 30, 58, 209, 188, 110, 80, 156, 185, 140, 89, 93, 211,
}
