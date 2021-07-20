# Transcoding Stream
# ==================

# Data Flow
# ---------
#
# When reading data (`state.mode == :read`):
#   user <--- |state.buffer1| <--- <codec> <--- |state.buffer2| <--- stream
#
# When writing data (`state.mode == :write`):
#   user ---> |state.buffer1| ---> <codec> ---> |state.buffer2| ---> stream

struct TranscodingStream{C<:Codec,S<:IO} <: IO
    # codec object
    codec::C

    # source/sink stream
    stream::S

    # mutable state of the stream
    state::State

    function TranscodingStream{C,S}(
            codec::C, stream::S, state::State, initialized::Bool) where {C<:Codec,S<:IO}
        if !isopen(stream)
            throw(ArgumentError("closed stream"))
        elseif state.mode != :idle
            throw(ArgumentError("invalid initial mode"))
        end
        if !initialized
            initialize(codec)
        end
        return new(codec, stream, state)
    end
end

function TranscodingStream(codec::C, stream::S, state::State;
                           initialized::Bool=false) where {C<:Codec,S<:IO}
    return TranscodingStream{C,S}(codec, stream, state, initialized)
end

const DEFAULT_BUFFER_SIZE = 16 * 2^10  # 16KiB

function checkbufsize(bufsize::Integer)
    if bufsize ≤ 0
        throw(ArgumentError("non-positive buffer size"))
    end
end

function checksharedbuf(sharedbuf::Bool, stream::IO)
    if sharedbuf && !(stream isa TranscodingStream)
        throw(ArgumentError("invalid stream type for sharedbuf=true"))
    end
end

"""
    TranscodingStream(codec::Codec, stream::IO;
                      bufsize::Integer=$(DEFAULT_BUFFER_SIZE),
                      stop_on_end::Bool=false,
                      sharedbuf::Bool=(stream isa TranscodingStream))

Create a transcoding stream with `codec` and `stream`.

A `TranscodingStream` object wraps an input/output stream object `stream`, and
transcodes the byte stream using `codec`. It is a subtype of `IO` and supports
most of the I/O functions in the standard library.

See the docs (<https://bicycle1885.github.io/TranscodingStreams.jl/stable/>) for
available codecs, examples, and more details of the type.

Arguments
---------

- `codec`:
    The data transcoder. The transcoding stream does the initialization and
    finalization of `codec`. Therefore, a codec object is not reusable once it
    is passed to a transcoding stream.
- `stream`:
    The wrapped stream. It must be opened before passed to the constructor.
- `bufsize`:
    The initial buffer size (the default size is 16KiB). The buffer may be
    extended whenever `codec` requests so.
- `stop_on_end`:
    The flag to stop transcoding on `:end` return code from `codec`.  The
    transcoded data are readable even after stopping transcoding process.  With
    this flag on, `stream` is not closed when the wrapper stream is closed with
    `close`.  Note that some extra data may be read from `stream` into an
    internal buffer, and thus `stream` must be a `TranscodingStream` object and
    `sharedbuf` must be `true` to reuse `stream`.
- `sharedbuf`:
    The flag to share buffers between adjacent transcoding streams.  The value
    must be `false` if `stream` is not a `TranscodingStream` object.

Examples
--------

```jldoctest
julia> using TranscodingStreams

julia> using CodecZlib

julia> file = open(joinpath(dirname(pathof(CodecZlib)), "..", "test", "abra.gz"));

julia> stream = TranscodingStream(GzipDecompressor(), file)
TranscodingStream{GzipDecompressor,IOStream}(<mode=idle>)

julia> String(read(stream))
"abracadabra"

julia> close(stream)

```
"""
function TranscodingStream(codec::Codec, stream::IO;
                           bufsize::Integer=DEFAULT_BUFFER_SIZE,
                           stop_on_end::Bool=false,
                           sharedbuf::Bool=(stream isa TranscodingStream))
    checkbufsize(bufsize)
    checksharedbuf(sharedbuf, stream)
    if sharedbuf
        stream::TranscodingStream
        state = State(Buffer(bufsize), stream.state.buffer1)
    else
        state = State(bufsize)
    end
    state.stop_on_end = stop_on_end
    return TranscodingStream(codec, stream, state)
end

function Base.show(io::IO, stream::TranscodingStream)
    print(io, summary(stream), "(<mode=$(stream.state.mode)>)")
end

# Split keyword arguments.
function splitkwargs(kwargs, keys)
    hits = []
    others = []
    for kwarg in kwargs
        push!(kwarg[1] ∈ keys ? hits : others, kwarg)
    end
    return hits, others
end

# Check that mode is valid.
macro checkmode(validmodes)
    mode = esc(:mode)
    validmodes = [arg.value for arg in validmodes.args]
    quote
        if !$(foldr((x, y) -> :($(mode) === $(QuoteNode(x)) || $(y)), validmodes, init=false))
            throw(ArgumentError(string("invalid mode :", $(mode))))
        end
    end
end


# Base IO Functions
# -----------------

function Base.open(f::Function, ::Type{T}, args...) where T<:TranscodingStream
    stream = T(open(args...))
    try
        f(stream)
    finally
        close(stream)
    end
end

function Base.isopen(stream::TranscodingStream)
    return stream.state.mode != :close && stream.state.mode != :panic
end

function Base.close(stream::TranscodingStream)
    stopped = stream.state.mode == :stop
    if stream.state.mode != :panic
        changemode!(stream, :close)
    end
    if !stopped
        close(stream.stream)
    end
    return nothing
end

function Base.eof(stream::TranscodingStream)
    mode = stream.state.mode
    if mode == :idle
        changemode!(stream, :read)
        return eof(stream)
    elseif mode == :read
        return buffersize(stream.state.buffer1) == 0 && fillbuffer(stream) == 0
    elseif mode == :write
        return eof(stream.stream)
    elseif mode == :close
        return true
    elseif mode == :stop
        return buffersize(stream.state.buffer1) == 0
    elseif mode == :panic
        throw_panic_error()
    else
        @assert false
    end
end

function Base.ismarked(stream::TranscodingStream)
    checkmode(stream)
    return ismarked(stream.state.buffer1)
end

function Base.mark(stream::TranscodingStream)
    checkmode(stream)
    return mark!(stream.state.buffer1)
end

function Base.unmark(stream::TranscodingStream)
    checkmode(stream)
    return unmark!(stream.state.buffer1)
end

function Base.reset(stream::TranscodingStream)
    checkmode(stream)
    return reset!(stream.state.buffer1)
end

function Base.skip(stream::TranscodingStream, offset::Integer)
    checkmode(stream)
    if offset < 0
        throw(ArgumentError("negative offset"))
    end
    mode = stream.state.mode
    buffer1 = stream.state.buffer1
    skipped = 0
    if mode == :read
        while !eof(stream) && buffersize(buffer1) < offset - skipped
            n = buffersize(buffer1)
            emptybuffer!(buffer1)
            skipped += n
        end
        if eof(stream)
            emptybuffer!(buffer1)
        else
            skipbuffer!(buffer1, offset - skipped)
        end
    else
        # TODO: support skip in write mode
        throw(ArgumentError("not in read mode"))
    end
    return
end

"""
    position(stream::TranscodingStream)

Return the number of bytes read from or written to `stream`.

Note that the returned value will be different from that of the underlying
stream wrapped by `stream`.  This is because `stream` buffers some data and the
codec may change the length of data.
"""
function Base.position(stream::TranscodingStream)
    mode = stream.state.mode
    @checkmode (:idle, :read, :write)
    if mode === :idle
        return Int64(0)
    elseif mode === :read
        return stats(stream).out
    elseif mode === :write
        return stats(stream).in
    end
    @assert false "unreachable"
end


# Seek Operations
# ---------------

function Base.seekstart(stream::TranscodingStream)
    mode = stream.state.mode
    @checkmode (:idle, :read, :write)
    if mode == :read || mode == :write
        callstartproc(stream, mode)
        emptybuffer!(stream.state.buffer1)
        emptybuffer!(stream.state.buffer2)
    end
    seekstart(stream.stream)
    return
end

function Base.seekend(stream::TranscodingStream)
    mode = stream.state.mode
    @checkmode (:idle, :read, :write)
    if mode == :read || mode == :write
        callstartproc(stream, mode)
        emptybuffer!(stream.state.buffer1)
        emptybuffer!(stream.state.buffer2)
    end
    seekend(stream.stream)
    return
end


# Read Functions
# --------------

function Base.read(stream::TranscodingStream, ::Type{UInt8})
    ready_to_read!(stream)
    if eof(stream)
        throw(EOFError())
    end
    return readbyte!(stream.state.buffer1)
end

function Base.readuntil(stream::TranscodingStream, delim::UInt8; keep::Bool=false)
    ready_to_read!(stream)
    buffer1 = stream.state.buffer1
    # delay initialization so as to reduce the number of buffer resizes
    local ret::Vector{UInt8}
    filled = 0
    while !eof(stream)
        p = findbyte(buffer1, delim)
        found = false
        if p < marginptr(buffer1)
            found = true
            sz = Int(p + 1 - bufferptr(buffer1))
            if !keep
                sz -= 1
            end
        else
            sz = buffersize(buffer1)
        end
        if @isdefined(ret)
            resize!(ret, filled + sz)
        else
            @assert filled == 0
            ret = Vector{UInt8}(undef, sz)
        end
        copydata!(pointer(ret, filled+1), buffer1, sz)
        filled += sz
        if found
            if !keep
                # skip the delimiter
                skipbuffer!(buffer1, 1)
            end
            break
        end
    end
    if !@isdefined(ret)
        # special case: stream is empty
        ret = UInt8[]
    end
    return ret
end

function Base.unsafe_read(stream::TranscodingStream, output::Ptr{UInt8}, nbytes::UInt)
    ready_to_read!(stream)
    buffer = stream.state.buffer1
    p = output
    p_end = output + nbytes
    while p < p_end && !eof(stream)
        m = min(buffersize(buffer), p_end - p)
        copydata!(p, buffer, m)
        p += m
    end
    if p < p_end && eof(stream)
        throw(EOFError())
    end
    return
end

function Base.readbytes!(stream::TranscodingStream, b::AbstractArray{UInt8}, nb=length(b))
    ready_to_read!(stream)
    filled = 0
    resized = false
    while filled < nb && !eof(stream)
        if length(b) == filled
            resize!(b, min(length(b) * 2, nb))
            resized = true
        end
        filled += unsafe_read(stream, pointer(b, filled+1), min(length(b), nb)-filled)
    end
    if resized
        resize!(b, filled)
    end
    return filled
end

function Base.bytesavailable(stream::TranscodingStream)
    ready_to_read!(stream)
    return buffersize(stream.state.buffer1)
end

function Base.readavailable(stream::TranscodingStream)
    n = bytesavailable(stream)
    data = Vector{UInt8}(undef, n)
    unsafe_read(stream, pointer(data), n)
    return data
end

"""
    unread(stream::TranscodingStream, data::Vector{UInt8})

Insert `data` to the current reading position of `stream`.

The next `read(stream, sizeof(data))` call will read data that are just
inserted.
"""
function unread(stream::TranscodingStream, data::ByteData)
    unsafe_unread(stream, pointer(data), sizeof(data))
end

"""
    unsafe_unread(stream::TranscodingStream, data::Ptr, nbytes::Integer)

Insert `nbytes` pointed by `data` to the current reading position of `stream`.

The data are copied into the internal buffer and hence `data` can be safely used
after the operation without interfering the stream.
"""
function unsafe_unread(stream::TranscodingStream, data::Ptr, nbytes::Integer)
    if nbytes < 0
        throw(ArgumentError("negative nbytes"))
    end
    ready_to_read!(stream)
    insertdata!(stream.state.buffer1, convert(Ptr{UInt8}, data), nbytes)
    return nothing
end

# Ready to read data from the stream.
function ready_to_read!(stream::TranscodingStream)
    mode = stream.state.mode
    if !(mode == :read || mode == :stop)
        changemode!(stream, :read)
    end
    return
end


# Write Functions
# ---------------

# Write nothing.
function Base.write(stream::TranscodingStream)
    changemode!(stream, :write)
    return 0
end

function Base.write(stream::TranscodingStream, b::UInt8)
    changemode!(stream, :write)
    if marginsize(stream.state.buffer1) == 0 && flushbuffer(stream) == 0
        return 0
    end
    return writebyte!(stream.state.buffer1, b)
end

function Base.unsafe_write(stream::TranscodingStream, input::Ptr{UInt8}, nbytes::UInt)
    changemode!(stream, :write)
    state = stream.state
    buffer1 = state.buffer1
    p = input
    p_end = p + nbytes
    while p < p_end && (marginsize(buffer1) > 0 || flushbuffer(stream) > 0)
        m = min(marginsize(buffer1), p_end - p)
        copydata!(buffer1, p, m)
        p += m
    end
    return Int(p - input)
end

# A singleton type of end token.
struct EndToken end

"""
A special token indicating the end of data.

`TOKEN_END` may be written to a transcoding stream like `write(stream,
TOKEN_END)`, which will terminate the current transcoding block.

!!! note

    Call `flush(stream)` after `write(stream, TOKEN_END)` to make sure that all
    data are written to the underlying stream.
"""
const TOKEN_END = EndToken()

function Base.write(stream::TranscodingStream, ::EndToken)
    changemode!(stream, :write)
    flushbufferall(stream)
    flushuntilend(stream)
    return 0
end

function Base.flush(stream::TranscodingStream)
    checkmode(stream)
    if stream.state.mode == :write
        flushbufferall(stream)
        writedata!(stream.stream, stream.state.buffer2)
    end
    flush(stream.stream)
end


# Stats
# -----

"""
I/O statistics.

Its object has four fields:
- `in`: the number of bytes supplied into the stream
- `out`: the number of bytes consumed out of the stream
- `transcoded_in`: the number of bytes transcoded from the input buffer
- `transcoded_out`: the number of bytes transcoded to the output buffer

Note that, since the transcoding stream does buffering, `in` is `transcoded_in +
{size of buffered data}` and `out` is `transcoded_out - {size of buffered
data}`.
"""
struct Stats
    in::Int64
    out::Int64
    transcoded_in::Int64
    transcoded_out::Int64
end

function Base.show(io::IO, stats::Stats)
    println(io, summary(stats), ':')
    println(io, "  in: ", stats.in)
    println(io, "  out: ", stats.out)
    println(io, "  transcoded_in: ", stats.transcoded_in)
      print(io, "  transcoded_out: ", stats.transcoded_out)
end

"""
    stats(stream::TranscodingStream)

Create an I/O statistics object of `stream`.
"""
function stats(stream::TranscodingStream)
    state = stream.state
    mode = state.mode
    @checkmode (:idle, :read, :write)
    buffer1 = state.buffer1
    buffer2 = state.buffer2
    if mode == :idle
        transcoded_in = transcoded_out = in = out = 0
    elseif mode == :read
        transcoded_in = buffer2.transcoded
        transcoded_out = buffer1.transcoded
        in = transcoded_in + buffersize(buffer2)
        out = transcoded_out - buffersize(buffer1)
    elseif mode == :write
        transcoded_in = buffer1.transcoded
        transcoded_out = buffer2.transcoded
        in = transcoded_in + buffersize(buffer1)
        out = transcoded_out - buffersize(buffer2)
    else
        @assert false "unreachable"
    end
    return Stats(in, out, transcoded_in, transcoded_out)
end


# Buffering
# ---------

function fillbuffer(stream::TranscodingStream; eager::Bool = false)
    changemode!(stream, :read)
    buffer1 = stream.state.buffer1
    buffer2 = stream.state.buffer2
    nfilled::Int = 0
    while ((!eager && buffersize(buffer1) == 0) || (eager && makemargin!(buffer1, 0, eager = true) > 0)) && stream.state.mode != :stop
        if stream.state.code == :end
            if buffersize(buffer2) == 0 && eof(stream.stream)
                break
            end
            callstartproc(stream, :read)
        end
        makemargin!(buffer2, 1)
        readdata!(stream.stream, buffer2)
        _, Δout = callprocess(stream, buffer2, buffer1)
        nfilled += Δout
    end
    return nfilled
end

function flushbuffer(stream::TranscodingStream, all::Bool=false)
    changemode!(stream, :write)
    state = stream.state
    buffer1 = state.buffer1
    buffer2 = state.buffer2
    nflushed::Int = 0
    while (all ? buffersize(buffer1) > 0 : makemargin!(buffer1, 0) == 0) && state.mode != :stop
        if state.code == :end
            callstartproc(stream, :write)
        end
        writedata!(stream.stream, buffer2)
        Δin, _ = callprocess(stream, buffer1, buffer2)
        nflushed += Δin
    end
    return nflushed
end

function flushbufferall(stream::TranscodingStream)
    return flushbuffer(stream, true)
end

function flushuntilend(stream::TranscodingStream)
    changemode!(stream, :write)
    state = stream.state
    buffer1 = state.buffer1
    buffer2 = state.buffer2
    while state.code != :end
        writedata!(stream.stream, buffer2)
        callprocess(stream, buffer1, buffer2)
    end
    writedata!(stream.stream, buffer2)
    @assert buffersize(buffer1) == 0
    return
end


# Interface to codec
# ------------------

# Call `startproc` with epilogne.
function callstartproc(stream::TranscodingStream, mode::Symbol)
    state = stream.state
    state.code = startproc(stream.codec, mode, state.error)
    if state.code == :error
        changemode!(stream, :panic)
    end
    return
end

# Call `process` with prologue and epilogue.
function callprocess(stream::TranscodingStream, inbuf::Buffer, outbuf::Buffer)
    state = stream.state
    input = buffermem(inbuf)
    makemargin!(outbuf, minoutsize(stream.codec, input))
    Δin, Δout, state.code = process(stream.codec, input, marginmem(outbuf), state.error)
    @debug(
        "called process()",
        code = state.code,
        input_size = buffersize(inbuf),
        output_size = marginsize(outbuf),
        input_delta = Δin,
        output_delta = Δout,
    )
    consumed!(inbuf, Δin, transcode = true)
    supplied!(outbuf, Δout, transcode = true)
    if state.code == :error
        changemode!(stream, :panic)
    elseif state.code == :ok && Δin == Δout == 0
        # When no progress, expand the output buffer.
        makemargin!(outbuf, max(16, marginsize(outbuf) * 2))
    elseif state.code == :end && state.stop_on_end
        changemode!(stream, :stop)
    end
    return Δin, Δout
end


# I/O operations
# --------------

# Read as much data as possbile from `input` to the margin of `output`.
# This function will not block if `input` has buffered data.
function readdata!(input::IO, output::Buffer)
    if input isa TranscodingStream && input.state.buffer1 === output
        # Delegate the operation to the underlying stream for shared buffers.
        return fillbuffer(input)
    end
    nread::Int = 0
    navail = bytesavailable(input)
    if navail == 0 && marginsize(output) > 0 && !eof(input)
        nread += writebyte!(output, read(input, UInt8))
        navail = bytesavailable(input)
    end
    n = min(navail, marginsize(output))
    Base.unsafe_read(input, marginptr(output), n)
    supplied!(output, n)
    nread += n
    return nread
end

# Write all data to `output` from the buffer of `input`.
function writedata!(output::IO, input::Buffer)
    if output isa TranscodingStream && output.state.buffer1 === input
        # Delegate the operation to the underlying stream for shared buffers.
        return flushbufferall(output)
    end
    nwritten::Int = 0
    while buffersize(input) > 0
        n = Base.unsafe_write(output, bufferptr(input), buffersize(input))
        consumed!(input, n)
        nwritten += n
    end
    return nwritten
end


# Mode Transition
# ---------------

# Change the current mode.
function changemode!(stream::TranscodingStream, newmode::Symbol)
    state = stream.state
    mode = state.mode
    buffer1 = state.buffer1
    buffer2 = state.buffer2
    if mode == newmode
        # mode does not change
        return
    elseif newmode == :panic
        if !haserror(state.error)
            set_default_error!(state.error)
        end
        state.mode = newmode
        finalize_codec(stream.codec, state.error)
        throw(state.error[])
    elseif mode == :idle
        if newmode == :read || newmode == :write
            state.code = startproc(stream.codec, newmode, state.error)
            if state.code == :error
                changemode!(stream, :panic)
            end
            state.mode = newmode
            return
        elseif newmode == :close
            state.mode = newmode
            finalize_codec(stream.codec, state.error)
            return
        end
    elseif mode == :read
        if newmode == :close || newmode == :stop
            state.mode = newmode
            finalize_codec(stream.codec, state.error)
            return
        end
    elseif mode == :write
        if newmode == :close || newmode == :stop
            if newmode == :close
                flushbufferall(stream)
                flushuntilend(stream)
            end
            state.mode = newmode
            finalize_codec(stream.codec, state.error)
            return
        end
    elseif mode == :stop
        if newmode == :close
            state.mode = newmode
            return
        end
    elseif mode == :panic
        throw_panic_error()
    end
    throw(ArgumentError("cannot change the mode from $(mode) to $(newmode)"))
end

# Check the current mode and throw an exception if needed.
function checkmode(stream::TranscodingStream)
    if stream.state.mode == :panic
        throw_panic_error()
    end
end

# Throw an argument error (must be called only when the mode is panic).
function throw_panic_error()
    throw(ArgumentError("stream is in unrecoverable error; only isopen and close are callable"))
end

# Set a defualt error.
function set_default_error!(error::Error)
    error[] = ErrorException("unknown error happened while processing data")
end

# Call the finalize method of the codec.
function finalize_codec(codec::Codec, error::Error)
    try
        finalize(codec)
    catch
        if haserror(error)
            throw(error[])
        else
            rethrow()
        end
    end
end
