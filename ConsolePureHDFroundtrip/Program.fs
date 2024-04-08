open System
open System.Runtime.InteropServices
open PureHDF
open PureHDF.Selections
open PureHDF.VOL.Native
open PureHDF.Filters

let file = System.IO.FileInfo(@"c:\tmp\roundtrip.h5")
let datasetName = "peaks"

[<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
type Peak =
    {
        mz : double
        ic : single
    }

let r = new Random(123)

let dim1Size = 11
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
                                | x when x < 0.85 -> 100
                                | x when x < 0.90 -> 300
                                | x when x < 0.95 -> 400
                                | _ -> 2

                            if count = 0 then
                                Unchecked.defaultof<_>
                            else
                                Array.init count (fun _ -> { mz = r.NextDouble(); ic = r.NextSingle() })
                        )
                )
        )

let dim1ChunkSize = 8
let dim2ChunkSize = 348
let dim3ChunkSize = 3
let chunkDims = [| uint32 dim1ChunkSize; uint32 dim2ChunkSize; uint32 dim3ChunkSize |]

//let cache =
//    PureHDF.VOL.Native.SimpleChunkCache(1024*1024, byteCount=3UL*1024UL*1024UL*1024UL)
//    :>IWritingChunkCache

let datasetCreation = VOL.Native.H5DatasetCreation(Filters = ResizeArray[H5Filter(DeflateFilter.Id)])
let dataset = H5Dataset<Peak[][,,]>(dims, chunkDims, datasetCreation = datasetCreation)

let hdfFile = H5File()
hdfFile[datasetName] <- dataset

let stream = file.Create()
let writer = hdfFile.BeginWrite(stream, H5WriteOptions(IncludeStructProperties=true))

for i = 0 to dim1Size - 1 do
    for j = 0 to dim2Size - 1 do
        let start = [| uint64 i; uint64 j; 0UL |]
        let count = [| 1UL;      1UL;      uint64 dim3Size |]
        let data = Array3D.init 1 1 dim3Size (fun _ _ k -> peaks[i].[j].[k]) // A 3D array with only the i,j,* entries (the variable-length Peak arrays)
        let fileSelection = HyperslabSelection(3, start, count)
        writer.Write<Peak[][,,]>(dataset, data, fileSelection = fileSelection)

writer.Dispose()
stream.Close()

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
