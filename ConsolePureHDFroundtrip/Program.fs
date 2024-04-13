open System
open System.Collections.Generic
open System.IO
open System.Runtime.InteropServices
open System.Diagnostics
open PureHDF
open PureHDF.Selections
open PureHDF.Filters
open PureHDF.VOL.Native

let datasetName = "peaks"

H5Filter.Register(Blosc2Filter())

[<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
type Peak =
    {
        mz : double
        ic : single
    }

let r = new Random(123)

let dim1Size = 26
let dim2Size = 953
let dim3Size = 17
let dims = [| uint64 dim1Size; uint64 dim2Size; uint64 dim3Size |]

// Data is stored as 3D array of variable-length lists, but source data is jagged
// Make a [][][] of Peak[] which are mostly length 0, some length 1, some length 2
let peaks =
    Array.init dim1Size
        (fun i ->
            Array.init dim2Size
                (fun j ->
                    Array.init dim3Size
                        (fun k ->
                            let count =
                                match r.NextDouble() with
                                | x when x < 0.85 -> 0
                                | x when x < 0.95 -> 1
                                | _ -> 2

                            Array.init count (fun _ -> { mz = r.NextDouble(); ic = r.NextSingle() })
                        )
                )
        )

let dim1ChunkSize = 26
let dim2ChunkSize = 174
let dim3ChunkSize = 3
let chunkDims = [| uint32 dim1ChunkSize; uint32 dim2ChunkSize; uint32 dim3ChunkSize |]

let saveDataSet (file : FileInfo) datasetCreation =
    let dataset = H5Dataset<Peak[][,,]>(dims, chunkDims, datasetCreation = datasetCreation)

    let hdfFile = H5File()
    hdfFile[datasetName] <- dataset

    let stream = file.Create()
    let writer = hdfFile.BeginWrite(stream, H5WriteOptions(IncludeStructProperties=true))

    for i in 0 .. dim1ChunkSize .. dim1Size - 1 do
        let ni = min dim1ChunkSize (dim1Size - i)
        for j in 0 .. dim2ChunkSize .. dim2Size - 1 do
            let nj = min dim2ChunkSize (dim2Size - j)
            for k in 0 .. dim3ChunkSize .. dim3Size - 1 do
                let nk = min dim3ChunkSize (dim3Size - k)

                let start = [| uint64 i; uint64 j; uint64 k |]
                let count = [| uint64 ni; uint64 nj; uint64 nk |]
                let data = Array3D.init ni nj nk (fun ii jj kk -> peaks[i+ii].[j+jj].[k+kk])
                let fileSelection = HyperslabSelection(3, start, count)
                writer.Write<Peak[][,,]>(dataset, data, fileSelection = fileSelection)

    writer.Dispose()
    stream.Close()

let readDataset (file : FileInfo) =
    let readDataset = H5File.Open(file.OpenRead()).Dataset(datasetName)
    let readDims = readDataset.Space.Dimensions
    let readDim1 = int readDims[0]
    let readDim2 = int readDims[1]
    let readDim3 = int readDims[2]

    if readDim1 <> dim1Size then failwithf "Dim1 mismatch: %d vs %d" readDim1 dim1Size
    if readDim2 <> dim2Size then failwithf "Dim2 mismatch: %d vs %d" readDim2 dim2Size
    if readDim3 <> dim3Size then failwithf "Dim3 mismatch: %d vs %d" readDim3 dim3Size

    let readBack =
        Array.init readDim1 (
            fun i ->
                Array.init readDim2 (
                    fun j ->
                        let starts =  [| uint64 i; uint64 j; 0UL |]
                        let stripDims = [| 1UL; 1UL; uint64 readDim3 |]
                        let fileSelection = HyperslabSelection(3, starts, stripDims)
                        let data : Peak[][,,] = readDataset.Read<Peak[][,,]>(fileSelection = fileSelection, memoryDims = stripDims)
                        data[0,0,*]
                )
        )
    readBack

let checkData readBack =
    (peaks, readBack)
    ||> Array.iter2 (
        fun expected actual ->
            (expected,actual)
            ||> Array.iter2 (
                fun expected actual ->
                    (expected,actual)
                    ||> Array.iter2 (
                        fun expected actual ->
                            (expected,actual)
                            ||> Array.iter2 (
                                fun expected actual ->
                                    if expected.mz <> actual.mz then failwithf "mz mismatch: %f vs %f" expected.mz actual.mz
                                    if expected.ic <> actual.ic then failwithf "ic mismatch: %f vs %f" expected.ic actual.ic
                            )
                    )
            )
    )

let baseFilename = @"c:\tmp\roundtrip"
let rawFile = System.IO.FileInfo(baseFilename + ".h5")
let shuffleFile = System.IO.FileInfo(baseFilename + "_shuffle.h5")
let bloscFile = System.IO.FileInfo(baseFilename + "_blosc.h5")

let sw = Stopwatch.StartNew()
saveDataSet rawFile (H5DatasetCreation())
printfn "Raw Write Time: %fs" sw.Elapsed.TotalSeconds

sw.Restart()
saveDataSet shuffleFile (H5DatasetCreation(Filters = ResizeArray[H5Filter(ShuffleFilter.Id)]))
printfn "Shuffle Write Time: %fs" sw.Elapsed.TotalSeconds

sw.Restart()
saveDataSet bloscFile (H5DatasetCreation(Filters = ResizeArray[H5Filter(Blosc2Filter.Id, Dictionary [KeyValuePair(Blosc2Filter.COMPRESSION_LEVEL, box 0)])]))
printfn "Blosc Write Time: %fs" sw.Elapsed.TotalSeconds

sw.Restart()
let rereadRaw = readDataset rawFile
printfn "Raw Read Time: %fs" sw.Elapsed.TotalSeconds

sw.Restart()
let rereadShuffle = readDataset shuffleFile
printfn "Shuffle Read Time: %fs" sw.Elapsed.TotalSeconds

sw.Restart()
let rereadBlosc = readDataset bloscFile
printfn "Blosc Read Time: %fs" sw.Elapsed.TotalSeconds

checkData rereadRaw
checkData rereadShuffle
checkData rereadBlosc
