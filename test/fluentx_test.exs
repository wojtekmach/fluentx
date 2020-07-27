defmodule FluentxTest do
  use ExUnit.Case, async: true

  @tag :capture_log
  test "fluentx" do
    self = self()
    {:ok, conn} = Fluentx.Client.start_link(after_connect: &send(self, {:connected, &1}))
    start_supervised!({Fluentx.Server, handler: &handler/3, log: &send(self, &1)})
    assert_receive {:connected, _}, 2000

    tag = "debug.test"
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    record = %{"foo" => "bar"}
    assert :ok = Fluentx.Client.send(conn, tag, now, record)
    assert_receive {^tag, ^now, ^record, {:handled, {^tag, ^now, ^record}}}
  end

  defp handler(tag, time, record) do
    {:handled, {tag, time, record}}
  end
end
