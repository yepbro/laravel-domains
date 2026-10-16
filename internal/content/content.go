// Package content — каркас хранилища контента (ТЗ 5.7).
//
// Интерфейс ContentStore (Put/Get/Delete), пак-файлы append-only,
// дедупликация по 128-битному хэшу, счётчики ссылок и компактификация —
// задачи T-041…T-046. Сейчас пакет даёт минимальные обёртки сжатия zstd,
// на которых строится запись в пак-файлы (обучаемый словарь — T-043).
package content

import (
	"github.com/klauspost/compress/zstd"
)

// Энкодер и декодер создаются один раз: EncodeAll/DecodeAll безопасны
// для одновременного использования из нескольких горутин.
var (
	encoder, _ = zstd.NewWriter(nil) // nil — только для EncodeAll
	decoder, _ = zstd.NewReader(nil) // nil — только для DecodeAll
)

// Compress сжимает src алгоритмом zstd (без словаря).
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, nil)
}

// Decompress распаковывает данные, сжатые Compress.
func Decompress(src []byte) ([]byte, error) {
	return decoder.DecodeAll(src, nil)
}
