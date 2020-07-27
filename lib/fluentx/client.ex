# TODO: support Forward mode
# TODO: support PackedForward mode
# TODO: support CompressedPackedForward mode
# TODO: support tls
# TODO: support heartbeat
# TODO: support handshake
# TODO: support json?
defmodule Fluentx.Client do
  use Connection
  require Logger

  defstruct [:sock, :host, :port, :connect_timeout, :socket_options, :after_connect]

  def start_link(options \\ []) do
    gen_server_options = Keyword.take(options, [:name])

    options =
      options
      |> Keyword.put_new(:host, System.get_env("FLUENT_HOST", "localhost"))
      |> Keyword.put_new(:port, 24224)
      |> Keyword.put_new(:connect_timeout, 5000)
      |> Keyword.put(:socket_options, [active: false] ++ (options[:socket_options] || []))

    Connection.start_link(__MODULE__, options, gen_server_options)
  end

  def child_spec(options) do
    %{
      id: Keyword.get(options, :name, :__MODULE__),
      start: {__MODULE__, :start_link, [options]}
    }
  end

  # TODO: rename
  def send(conn, tag, datetime, record) do
    # TODO: represent as EventTime to get microsecond precision
    # https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1#eventtime-ext-format
    time = DateTime.to_unix(datetime)
    data = Msgpax.pack!([tag, time, record])
    Connection.call(conn, {:send, data})
  end

  @impl true
  def init(options) do
    options = Keyword.drop(options, [:name])
    state = struct!(__MODULE__, options)
    {:connect, :init, state}
  end

  @impl true
  def connect(_, state) do
    socket_options = [active: false] ++ state.socket_options

    case :gen_tcp.connect(
           String.to_charlist(state.host),
           state.port,
           socket_options,
           state.connect_timeout
         ) do
      {:ok, sock} ->
        state = %{state | sock: sock}
        if state.after_connect, do: state.after_connect.(state)
        {:ok, state}

      {:error, reason} ->
        Logger.error("#{inspect(__MODULE__)} connect error: #{inspect(reason)}")
        # TODO: exp + jitter backoff
        {:backoff, 200, state}
    end
  end

  @impl true
  def disconnect(info, state) do
    :ok = :gen_tcp.close(state.sock)
    Logger.error("#{inspect(__MODULE__)} disconnect #{inspect(info)}")
    {:connect, :reconnect, %{state | sock: nil}}
  end

  @impl true
  def handle_call(_, _, %{sock: nil} = state) do
    {:reply, {:error, :closed}, state}
  end

  def handle_call({:send, data}, _from, state) do
    case :gen_tcp.send(state.sock, data) do
      :ok ->
        {:reply, :ok, state}

      {:error, _} = error ->
        {:disconnect, error, error, state}
    end
  end
end
