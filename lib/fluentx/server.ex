defmodule Fluentx.Server do
  @behaviour :ranch_protocol

  require Logger

  defstruct [:ref, :socket, :transport, :log]

  def child_spec(opts) do
    transport_opts = [port: 24224]
    :ranch.child_spec(__MODULE__, :ranch_tcp, transport_opts, __MODULE__, opts)
  end

  @impl :ranch_protocol
  def start_link(ref, _, transport, opts) do
    pid = :proc_lib.spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  def init(ref, transport, opts) do
    {:ok, socket} = :ranch.handshake(ref)
    handler = Keyword.fetch!(opts, :handler)

    state = %__MODULE__{
      ref: ref,
      socket: socket,
      transport: transport,
      log: opts[:log]
    }

    loop(handler, state)
  end

  defp loop(handler, state) do
    case state.transport.recv(state.socket, 0, :infinity) do
      {:ok, data} ->
        [tag, time, record] = Msgpax.unpack!(data, ext: Fluentx.Unpacker) |> List.flatten()

        time =
          case time do
            %DateTime{} -> time
            integer when is_integer(time) -> DateTime.from_unix!(integer)
          end

        result = handler.(tag, time, record)
        if state.log, do: state.log.({tag, time, record, result})
        loop(handler, state)

      {:error, _} = error ->
        Logger.error("#{inspect(__MODULE__)} error: #{inspect(error)}")
    end
  end
end

defmodule Fluentx.Unpacker do
  @moduledoc false
  @behaviour Msgpax.Ext.Unpacker
  @rep_byte_ext_type 0

  # https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
  def unpack(%Msgpax.Ext{type: @rep_byte_ext_type, data: <<second::32, nanosecond::32>>}) do
    microsecond = div(nanosecond, 1000)
    datetime = DateTime.from_unix!(second * 1_000_000 + microsecond, :microsecond)
    {:ok, datetime}
  end
end
