defmodule Anubis.Server.Transport.StreamableHTTP.PlugPersistenceTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Anubis.Server.Registry.Local
  alias Anubis.Server.Supervisor, as: ServerSupervisor
  alias Anubis.Server.Transport.StreamableHTTP.Plug
  alias Anubis.Test.MockSessionStore

  setup do
    {:ok, _} = MockSessionStore.start_link([])
    MockSessionStore.reset!()

    original_config = Application.get_env(:anubis_mcp, :session_store)

    Application.put_env(:anubis_mcp, :session_store,
      enabled: true,
      adapter: MockSessionStore,
      ttl: 1_800_000
    )

    on_exit(fn ->
      if original_config do
        Application.put_env(:anubis_mcp, :session_store, original_config)
      else
        Application.delete_env(:anubis_mcp, :session_store)
      end
    end)

    :ok
  end

  describe "session persistence without tokens" do
    setup do
      session_config = %{
        server_module: StubServer,
        registry_mod: Anubis.Server.Registry.None,
        transport: [layer: StubTransport, name: :stub_transport],
        session_idle_timeout: nil,
        timeout: 30_000,
        task_supervisor: :test_task_sup
      }

      :persistent_term.put({ServerSupervisor, StubServer, :session_config}, session_config)

      on_exit(fn ->
        :persistent_term.erase({ServerSupervisor, StubServer, :session_config})
      end)

      plug_opts = Plug.init(server: StubServer, session_header: "mcp-session-id")
      {:ok, plug_opts: plug_opts}
    end

    test "GET request with existing session ID reconnects to stored session", %{plug_opts: _plug_opts} do
      session_id = "existing_session_123"

      session_data = %{
        id: session_id,
        protocol_version: "2024-11-21",
        initialized: true,
        client_info: %{"name" => "test_client"}
      }

      :ok = MockSessionStore.save(session_id, session_data, [])

      conn =
        :get
        |> conn("/")
        |> put_req_header("accept", "text/event-stream")
        |> put_req_header("mcp-session-id", session_id)

      assert get_req_header(conn, "mcp-session-id") == [session_id]

      assert {:ok, stored_data} = MockSessionStore.load(session_id, [])
      assert stored_data.id == session_id
      assert stored_data.initialized == true
    end

    test "GET request without session ID generates new session", %{plug_opts: _plug_opts} do
      conn =
        :get
        |> conn("/")
        |> put_req_header("accept", "text/event-stream")

      assert get_req_header(conn, "mcp-session-id") == []
    end

    test "POST request with existing session ID uses stored session", %{plug_opts: _plug_opts} do
      session_id = "post_session_111"

      session_data = %{
        id: session_id,
        protocol_version: "2024-11-21",
        initialized: true
      }

      :ok = MockSessionStore.save(session_id, session_data, [])

      message = %{
        "jsonrpc" => "2.0",
        "method" => "tools/list",
        "id" => "req_1"
      }

      conn =
        :post
        |> conn("/", JSON.encode!(message))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("accept", "application/json, text/event-stream")
        |> put_req_header("mcp-session-id", session_id)

      assert get_req_header(conn, "mcp-session-id") == [session_id]
    end
  end

  describe "session lifecycle with persistence" do
    setup do
      task_sup = :"test_task_sup_#{System.unique_integer([:positive])}"
      transport_name = :"test_transport_#{System.unique_integer([:positive])}"

      start_supervised!({Task.Supervisor, name: task_sup})
      start_supervised!({StubTransport, name: transport_name})

      %{task_supervisor: task_sup, transport_name: transport_name}
    end

    test "initialize request triggers session persistence", ctx do
      session_id = "persist_init_#{System.unique_integer([:positive])}"
      session_name = :"session_#{session_id}"

      {:ok, session} =
        start_supervised(
          {Anubis.Server.Session,
           session_id: session_id,
           server_module: StubServer,
           name: session_name,
           transport: [layer: StubTransport, name: ctx.transport_name],
           task_supervisor: ctx.task_supervisor}
        )

      init_request = %{
        "jsonrpc" => "2.0",
        "method" => "initialize",
        "params" => %{
          "protocolVersion" => "2025-03-26",
          "capabilities" => %{},
          "clientInfo" => %{
            "name" => "test_client",
            "version" => "1.0.0"
          }
        },
        "id" => "init_1"
      }

      {:ok, _response} = GenServer.call(session, {:mcp_request, init_request, %{}})

      {:ok, stored} = MockSessionStore.load(session_id, [])
      assert stored.id == session_id
      assert stored.protocol_version == "2025-03-26"
      assert stored.client_info == %{"name" => "test_client", "version" => "1.0.0"}
    end

    test "session data can be updated in store" do
      session_id = "update_session_222"

      initial_data = %{
        id: session_id,
        initialized: false,
        log_level: "info"
      }

      :ok = MockSessionStore.save(session_id, initial_data, [])

      updates = %{
        initialized: true,
        log_level: "debug"
      }

      :ok = MockSessionStore.update(session_id, updates, [])

      {:ok, updated_data} = MockSessionStore.load(session_id, [])
      assert updated_data.initialized == true
      assert updated_data.log_level == "debug"
      assert updated_data.id == session_id
    end

    test "DELETE request removes session from store" do
      session_id = "delete_session_333"

      session_data = %{
        id: session_id,
        initialized: true
      }

      :ok = MockSessionStore.save(session_id, session_data, [])

      assert {:ok, _} = MockSessionStore.load(session_id, [])

      conn =
        :delete
        |> conn("/")
        |> put_req_header("mcp-session-id", session_id)

      :ok = MockSessionStore.delete(session_id, [])

      assert {:error, :not_found} = MockSessionStore.load(session_id, [])
      assert get_req_header(conn, "mcp-session-id") == [session_id]
    end

    test "can list all active sessions" do
      session_ids = ["session_a", "session_b", "session_c"]

      for session_id <- session_ids do
        :ok = MockSessionStore.save(session_id, %{id: session_id}, [])
      end

      {:ok, active} = MockSessionStore.list_active([])
      assert length(active) == 3
      assert Enum.all?(session_ids, &(&1 in active))
    end
  end

  describe "cross-node session restoration" do
    setup do
      task_sup = :"test_task_sup_#{System.unique_integer([:positive])}"
      transport_name = :"test_transport_#{System.unique_integer([:positive])}"

      start_supervised!({Task.Supervisor, name: task_sup})
      start_supervised!({StubTransport, name: transport_name})

      registry_name = Anubis.Server.Registry.registry_name(StubServer)
      start_supervised!({Local, name: registry_name})

      session_config = %{
        server_module: StubServer,
        registry_mod: Local,
        transport: [layer: StubTransport, name: transport_name],
        session_idle_timeout: nil,
        timeout: 30_000,
        task_supervisor: task_sup
      }

      :persistent_term.put({ServerSupervisor, StubServer, :session_config}, session_config)

      session_sup_name = Anubis.Server.Registry.session_supervisor_name(StubServer)
      start_supervised!({DynamicSupervisor, name: session_sup_name, strategy: :one_for_one})

      streamable_http_name = Anubis.Server.Registry.transport_name(StubServer, :streamable_http)

      start_supervised!(
        {Anubis.Server.Transport.StreamableHTTP,
         server: StubServer, name: streamable_http_name, task_supervisor: task_sup}
      )

      plug_opts = Plug.init(server: StubServer)

      on_exit(fn ->
        :persistent_term.erase({ServerSupervisor, StubServer, :session_config})
      end)

      %{
        plug_opts: plug_opts,
        session_config: session_config,
        task_sup: task_sup
      }
    end

    test "restores session from store when not found locally for a request", %{
      plug_opts: plug_opts,
      session_config: session_config,
      task_sup: task_sup
    } do
      # Step 1: Create a session, initialize it, persist it, then kill the local process
      # to simulate the request landing on a different dyno.
      session_id = "cross-node-#{System.unique_integer([:positive])}"
      session_name = Anubis.Server.Registry.session_name(StubServer, session_id)

      {:ok, session_pid} =
        ServerSupervisor.start_session(StubServer,
          session_id: session_id,
          server_module: StubServer,
          name: session_name,
          transport: session_config.transport,
          session_idle_timeout: 1_800_000,
          timeout: 30_000,
          task_supervisor: task_sup
        )

      registry_name = Anubis.Server.Registry.registry_name(StubServer)
      Local.register_session(registry_name, session_id, session_pid)

      # Initialize the session
      init_req = %{
        "jsonrpc" => "2.0",
        "id" => "init_1",
        "method" => "initialize",
        "params" => %{
          "protocolVersion" => "2025-03-26",
          "clientInfo" => %{"name" => "test_client", "version" => "1.0.0"},
          "capabilities" => %{}
        }
      }

      {:ok, _response} = GenServer.call(session_pid, {:mcp_request, init_req, %{}})

      GenServer.cast(
        session_pid,
        {:mcp_notification, %{"jsonrpc" => "2.0", "method" => "notifications/initialized"}, %{}}
      )

      Process.sleep(30)

      # Verify session was persisted to the store
      assert {:ok, _saved} = MockSessionStore.load(session_id, [])

      # Step 2: Kill the session process to simulate a different dyno
      DynamicSupervisor.terminate_child(
        Anubis.Server.Registry.session_supervisor_name(StubServer),
        session_pid
      )

      Process.sleep(30)

      # Verify local registry no longer has it
      assert {:error, :not_found} =
               Local.lookup_session(registry_name, session_id)

      # Step 3: Send a request through the Plug with the session ID
      # This should restore the session from the mock store and handle the request
      request = %{
        "jsonrpc" => "2.0",
        "id" => "ping_1",
        "method" => "ping",
        "params" => %{}
      }

      conn =
        :post
        |> conn("/", JSON.encode!(request))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("accept", "application/json")
        |> put_req_header("mcp-session-id", session_id)
        |> Plug.call(plug_opts)

      assert conn.status == 200
      {:ok, response} = JSON.decode(conn.resp_body)
      assert response["result"] == %{}
      assert response["id"] == "ping_1"
    end

    test "restores session from store for notifications", %{
      plug_opts: plug_opts,
      session_config: session_config,
      task_sup: task_sup
    } do
      session_id = "cross-node-notif-#{System.unique_integer([:positive])}"
      session_name = Anubis.Server.Registry.session_name(StubServer, session_id)

      {:ok, session_pid} =
        ServerSupervisor.start_session(StubServer,
          session_id: session_id,
          server_module: StubServer,
          name: session_name,
          transport: session_config.transport,
          session_idle_timeout: 1_800_000,
          timeout: 30_000,
          task_supervisor: task_sup
        )

      registry_name = Anubis.Server.Registry.registry_name(StubServer)
      Local.register_session(registry_name, session_id, session_pid)

      init_req = %{
        "jsonrpc" => "2.0",
        "id" => "init_1",
        "method" => "initialize",
        "params" => %{
          "protocolVersion" => "2025-03-26",
          "clientInfo" => %{"name" => "test_client", "version" => "1.0.0"},
          "capabilities" => %{}
        }
      }

      {:ok, _response} = GenServer.call(session_pid, {:mcp_request, init_req, %{}})

      GenServer.cast(
        session_pid,
        {:mcp_notification, %{"jsonrpc" => "2.0", "method" => "notifications/initialized"}, %{}}
      )

      Process.sleep(30)

      # Kill session to simulate different dyno
      DynamicSupervisor.terminate_child(
        Anubis.Server.Registry.session_supervisor_name(StubServer),
        session_pid
      )

      Process.sleep(30)

      # Send a notification - should restore and return 202
      notification = %{
        "jsonrpc" => "2.0",
        "method" => "notifications/message",
        "params" => %{"level" => "info", "data" => "test"}
      }

      conn =
        :post
        |> conn("/", JSON.encode!(notification))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("accept", "application/json")
        |> put_req_header("mcp-session-id", session_id)
        |> Plug.call(plug_opts)

      assert conn.status == 202
    end

    test "returns error when session not in store either", %{plug_opts: plug_opts} do
      request = %{
        "jsonrpc" => "2.0",
        "id" => "ping_1",
        "method" => "ping",
        "params" => %{}
      }

      conn =
        :post
        |> conn("/", JSON.encode!(request))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("accept", "application/json")
        |> put_req_header("mcp-session-id", "totally-unknown-session")
        |> Plug.call(plug_opts)

      assert conn.status == 400
    end
  end

  describe "backward compatibility" do
    test "sessions work exactly as before when no store is configured" do
      Application.delete_env(:anubis_mcp, :session_store)

      message = %{
        "jsonrpc" => "2.0",
        "method" => "ping",
        "id" => "ping_1"
      }

      conn =
        :post
        |> conn("/", JSON.encode!(message))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("accept", "application/json")

      assert conn.request_path == "/"
      assert get_req_header(conn, "content-type") == ["application/json"]
    end

    test "clients without session IDs work normally" do
      conn =
        :get
        |> conn("/")
        |> put_req_header("accept", "text/event-stream")

      assert conn.request_path == "/"
    end

    test "clients with session IDs reconnect transparently" do
      session_id = "client_session_999"

      :ok =
        MockSessionStore.save(
          session_id,
          %{
            id: session_id,
            initialized: true,
            protocol_version: "2024-11-21"
          },
          []
        )

      conn =
        :get
        |> conn("/")
        |> put_req_header("accept", "text/event-stream")
        |> put_req_header("mcp-session-id", session_id)

      assert get_req_header(conn, "mcp-session-id") == [session_id]
    end
  end
end
