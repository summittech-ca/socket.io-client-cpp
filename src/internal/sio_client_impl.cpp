
//
//  sio_client_impl.cpp
//  SioChatDemo
//
//  Created by Melo Yao on 4/3/15.
//  Copyright (c) 2015 Melo Yao. All rights reserved.
//

#include "sio_client_impl.h"
#include <cinttypes>
#include <functional>
#include <sstream>
#include <chrono>
#include <mutex>
#include <cmath>
#include <stdio.h>
// Comment this out to disable handshake logging to stdout

#ifndef _SAL_TIME_H
#define SAL_FUNC_DEBUG cout<<
#define SAL_FUNC_VERBOSE cout<<
#define SAL_FUNC_INFO cout<<
#define SAL_FUNC_WARN cout<<
#define SAL_FUNC_ERROR cout<<
#else
#include "SAL/Log/Log.h"
#include "SAL/OS/SocketQueue.h"
#include "SAL/OS/Sync.h"

namespace {
    LOGGER_NAME("socketio.client")
}

SAL::TimerManagerPtr getTimerMgr();

#endif

namespace sio
{
    /*************************public:*************************/
    client_impl::client_impl(ProtocolVersion version) :
        m_ping_interval(0),
        m_ping_timeout(0),
        m_network_thread(),
        m_con_state(con_closed),
        m_reconn_delay(5000),
        m_reconn_delay_max(25000),
        m_reconn_attempts(0xFFFFFFFF),
        m_reconn_made(0),
        m_protocol_version(version)
    {
        using websocketpp::log::alevel;
        m_client.clear_access_channels(alevel::frame_header|alevel::frame_payload);
#ifndef DEBUG
        m_client.clear_access_channels(alevel::all);
        m_client.set_access_channels(alevel::connect|alevel::disconnect|alevel::app);
#endif
#ifndef _SAL_TIME_H
        // Initialize the Asio transport policy
        m_client.init_asio();
#endif
        // Bind the clients we are using
        using std::placeholders::_1;
        using std::placeholders::_2;
        m_client.set_open_handler(std::bind(&client_impl::on_open,this,_1));
        m_client.set_close_handler(std::bind(&client_impl::on_close,this,_1));
        m_client.set_fail_handler(std::bind(&client_impl::on_fail,this,_1));
        m_client.set_message_handler(std::bind(&client_impl::on_message,this,_1,_2));
#if SIO_TLS
        m_client.set_tls_init_handler(std::bind(&client_impl::on_tls_init,this,_1));
#endif
        m_packet_mgr.set_decode_callback(std::bind(&client_impl::on_decode,this,_1));

        m_packet_mgr.set_encode_callback(std::bind(&client_impl::on_encode,this,_1,_2));
    }

    client_impl::~client_impl()
    {
        this->sockets_invoke_void(&sio::socket::on_close);
        sync_close();
    }

    void client_impl::connect(const string& uri, const map<string,string>& query, const map<string, string>& headers, const message::ptr& auth)
    {
#ifdef _SAL_TIME_H
        // Connection state is owned by the socket-queue thread (transport callbacks
        // and timer handlers run there too); callers may be on any thread.
        run_on_queue([this, uri, query, headers, auth]() {
            this->connect_on_queue(uri, query, headers, auth);
        });
#else
        connect_on_queue(uri, query, headers, auth);
#endif
    }

    void client_impl::connect_on_queue(const string& uri, const map<string,string>& query, const map<string, string>& headers, const message::ptr& auth)
    {
        reset_timer(m_reconn_timer);
        if(m_network_thread)
        {
            if(m_con_state == con_closing||m_con_state == con_closed)
            {
                //if client is closing, join to wait.
                //if client is closed, still need to join,
                //but in closed case,join will return immediately.
                m_network_thread->join();
                m_network_thread.reset();//defensive
            }
            else
            {
                //if we are connected, do nothing.
                return;
            }
        }
        m_con_state = con_opening;
        m_base_url = uri;
        m_reconn_made = 0;

        string query_str;
        for(map<string,string>::const_iterator it=query.begin();it!=query.end();++it){
            query_str.append("&");
            query_str.append(it->first);
            query_str.append("=");
            string query_str_value=encode_query_string(it->second);
            query_str.append(query_str_value);
        }
        m_query_string=std::move(query_str);
        m_http_headers = headers;
        m_auth = auth;

        // On _SAL_TIME_H builds m_network_thread is never set, so the "already connected"
        // early return above never runs and a connect() during a live or still-closing
        // connection used to open a second websocket beside it. Both then fed the one packet
        // manager and namespace map, so the router's kick of the older one closed the
        // namespace sockets of the newer one.
        this->retire_current_connection();
        this->reset_states();
        m_abort_retries = false;
#ifdef _SAL_TIME_H
        this->connect_impl(uri, m_query_string);
#else
        m_client.get_io_service().dispatch(std::bind(&client_impl::connect_impl,this,uri,m_query_string));
        m_network_thread.reset(new thread(std::bind(&client_impl::run_loop,this)));//uri lifecycle?
#endif
    }

    socket::ptr client_impl::socket(string const& nsp)
    {
        string aux;
        if(nsp == "")
        {
            aux = "/";
        }
        else if( nsp[0] != '/')
        {
            aux.append("/",1);
            aux.append(nsp);
        }
        else
        {
            aux = nsp;
        }

        {
            lock_guard<mutex> guard(m_socket_mutex);
            auto it = m_sockets.find(aux);
            SAL_FUNC_INFO("%d", it!= m_sockets.end());
            if(it!= m_sockets.end())
            {
                return it->second;
            }
        }

        // Constructed with m_socket_mutex released. The sio::socket constructor calls
        // send_connect(), whose write can fail synchronously (ECONNABORTED on a half-dead
        // connection) and re-enter client_impl::on_close() on this very thread. on_close()
        // takes m_socket_mutex via sockets_invoke_void(), so building this under the lock
        // deadlocks the non-recursive mutex against itself and wedges the client forever.
        socket::ptr s(new sio::socket(this,aux,m_auth));

        // Publish only a socket whose CONNECT actually went out. A cached socket is handed
        // straight back on every later call and never re-sends (socket::impl::on_open()
        // deliberately does not send_connect), so caching a failed one would poison the
        // namespace until remove_socket(). Leaving it out means the next call reconstructs
        // and retries, which is what the reconnect path relies on.
        if(!opened())
        {
            return s;
        }

        lock_guard<mutex> guard(m_socket_mutex);
        auto it = m_sockets.find(aux);
        if(it!= m_sockets.end())
        {
            return it->second;
        }
        return m_sockets.insert(pair<const string, socket::ptr>(aux,s)).first->second;
    }

    void client_impl::close()
    {
        // Captured before the namespace sockets close: closing "/" re-enters on_close,
        // which resets m_con.
        connection_hdl target = m_con;
        m_con_state = con_closing;
        m_abort_retries = true;
        this->sockets_invoke_void(&sio::socket::close);
#ifdef _SAL_TIME_H
        // The websocket transport drives I/O on the socket-queue thread, so the
        // close must run there too — otherwise it races with inbound frame
        // handling. Post() runs inline when already on that thread (the common
        // case here: close() is invoked from on_fail/on_open).
        SAL_FUNC_INFO("close(): posting close_impl to socket-queue thread");
        SAL::MSocketQueueManager::Post([this, target]() {
            SAL_FUNC_INFO("close(): close_impl running on socket-queue thread");
            this->close_impl(target, close::status::normal, "End by user");
        });
#else
        close_impl(target, close::status::normal,"End by user");
#endif
    }

    void client_impl::sync_close()
    {
        m_con_state = con_closing;
        m_abort_retries = true;
        this->sockets_invoke_void(&sio::socket::close);
#ifdef _SAL_TIME_H
        // Called from ~client_impl: the endpoint is about to be destroyed.
        // Run close_impl on the socket-queue thread (serialized with inbound
        // frame handling) and block until it has run, so no SocketQueueKpoll
        // callback can touch this connection after we return. Run inline if we
        // are already on that thread, to avoid self-deadlock.
        if (SAL::MSocketQueueManager::IsOnQueueThread())
        {
            SAL_FUNC_INFO("sync_close: force-terminating inline (already on socket-queue thread)");
            force_close_impl();
            *m_alive = false;
            SAL_FUNC_INFO("sync_close: close completed (inline)");
        }
        else
        {
            SAL_FUNC_INFO("sync_close: dispatching force-close to socket-queue thread and waiting");
            SAL::Event done(false);
            SAL::MSocketQueueManager::Post([this, &done]() {
                SAL_FUNC_INFO("sync_close: force_close_impl running on socket-queue thread");
                this->force_close_impl();
                // Work posted after this point (late timers, DNS replies) must not touch us.
                *this->m_alive = false;
                done.signal();
            });
            // Bounded wait. A live loop runs the task within its 1s heartbeat;
            // the timeout only guards against a wedged/dead loop so teardown
            // can't block forever.
            if (!done.wait(5000))
            {
                SAL_FUNC_ERROR("sync_close: timed out waiting for socket-queue close");
            }
            else
            {
                SAL_FUNC_INFO("sync_close: close completed on socket-queue thread");
            }
        }
#else
        close_impl(close::status::normal,"End by user");
#endif
        if(m_network_thread)
        {
            m_network_thread->join();
            m_network_thread.reset();
        }
    }

    void client_impl::set_logs_default()
    {
        m_client.clear_access_channels(websocketpp::log::alevel::all);
        m_client.set_access_channels(websocketpp::log::alevel::connect | websocketpp::log::alevel::disconnect | websocketpp::log::alevel::app);
    }

    void client_impl::set_logs_quiet()
    {
        m_client.clear_access_channels(websocketpp::log::alevel::all);
    }

    void client_impl::set_logs_verbose()
    {
        m_client.set_access_channels(websocketpp::log::alevel::all);
    }

    /*************************protected:*************************/
    void client_impl::send(packet& p)
    {
        m_packet_mgr.encode(p);
    }

    void client_impl::remove_socket(string const& nsp)
    {
        lock_guard<mutex> guard(m_socket_mutex);
        auto it = m_sockets.find(nsp);
        if(it!= m_sockets.end())
        {
            m_sockets.erase(it);
        }
    }
#ifndef _SAL_TIME_H
    asio::io_service& client_impl::get_io_service()
    {
		return m_client.get_io_service();
    }
#endif
    void client_impl::on_socket_closed(string const& nsp)
    {
        if(m_socket_close_listener)m_socket_close_listener(nsp);
        SAL_FUNC_INFO("on_close if nsp==/");
        // m_con is already empty when "/" closes as part of a reconnect or after the
        // connection itself closed; there is nothing left to close then.
        if (nsp == "/" && !m_con.expired()) on_close(m_con);
    }

    void client_impl::on_socket_opened(string const& nsp)
    {
        if(m_socket_open_listener)m_socket_open_listener(nsp);
    }

    /*************************private:*************************/
    void client_impl::run_loop()
    {
#ifndef _SAL_TIME_H
        m_client.run();
        m_client.reset();
        m_client.get_alog().write(websocketpp::log::alevel::devel,
                                   "run loop end");
#endif
    }

    void client_impl::connect_impl(const string& uri, const string& queryString)
    {
        do{
            websocketpp::uri uo(uri);
            ostringstream ss;
            if (uo.get_secure()) {
                ss<<"wss://";
            } else {
                ss<<"ws://";
            }
            const std::string host(uo.get_host());
            // As per RFC2732, literal IPv6 address should be enclosed in "[" and "]".
            if(host.find(':')!=std::string::npos){
                ss<<"["<<uo.get_host()<<"]";
            } else {
                ss<<uo.get_host();
            }

            // If a resource path was included in the URI, use that, otherwise
            // use the default /socket.io/.
            const std::string path(uo.get_resource() == "/" ? "/socket.io/" : uo.get_resource());
            int proto = (int)m_protocol_version;

            ss<<":"<<uo.get_port()<<path<<"?EIO="<<proto<<"&transport=websocket";
            if(m_sid.size()>0){
                ss<<"&sid="<<m_sid;
            }
            ss<<"&t="<<time(NULL)<<queryString;
            lib::error_code ec;
            client_type::connection_ptr con = m_client.get_connection(ss.str(), ec);
            if (ec) {
                m_client.get_alog().write(websocketpp::log::alevel::app,
                                          "Get Connection Error: "+ec.message());
                break;
            }

            for( auto&& header: m_http_headers ) {
                con->replace_header(header.first, header.second);
            }

            // Capture the connection NOW, before connect() registers its socket
            // with the socket-queue loop. on_open (where m_con_strong used to be
            // set) only fires once the handshake completes, so a client_impl torn
            // down during the connecting/handshaking window would leave this
            // socket registered with a handler bound to the freed client_impl —
            // a later inbound frame then lands in put_payload on freed memory
            // (ON-1531 reopen). Holding the strong ref here lets force_close_impl
            // always reach this connection and terminate() it (→ async_shutdown →
            // UnregisterHandler) before the object is freed.
            {
                std::lock_guard<std::mutex> lk(m_con_strong_mutex);
                m_con_strong = con;
                m_current_con = con;
            }

            m_client.connect(con);
            return;
        }
        while(0);
        if(m_fail_listener)
        {
            m_fail_listener();
        }
    }

    void client_impl::close_impl(close::status::value const& code,string const& reason)
    {
        close_impl(m_con, code, reason);
    }

    void client_impl::close_impl(connection_hdl con, close::status::value const& code, string const& reason)
    {
        SAL_FUNC_INFO("Close by reason: %s", reason.c_str());
        if (con.expired())
        {
            SAL_FUNC_ERROR("Error: No active session");
            // Between reconnect attempts there is no session, and the pending
            // m_reconn_timer is the only thing that will ever bring one back. Only a
            // user close (m_abort_retries) may cancel it; an internal close landing
            // here would leave the client closed for good.
            if (m_abort_retries)
            {
                // A reconnect may still be opening (m_con is only set in on_open):
                // terminate it, or it stays open and keeps dispatching events after
                // the close. Cancels m_reconn_timer too.
                force_close_impl();
            }
            return;
        }
        reset_timer(m_reconn_timer);
        lib::error_code ec;
        m_client.close(con, code, reason, ec);
        if(ec)
        {
            SAL_FUNC_WARN("close failed,reason: %d / %s", (int)ec.value(), ec.message().c_str()); // linux.socket: write(): send(143) failed with error 32
            this->on_close(con); // force to closed even fail to avoid timeout_connection if reuse as con_opened
        }
    }

    void client_impl::retire_current_connection()
    {
        client_type::connection_ptr old;
        {
            std::lock_guard<std::mutex> lk(m_con_strong_mutex);
            old = m_con_strong;
            m_con_strong.reset();
            m_current_con.reset();
        }
        m_con.reset();
        if (!old)
        {
            return;
        }

        // terminate(), not a graceful close: a graceful close only sends the close frame and
        // keeps the socket open and readable until the server answers, which it may not do
        // for a minute. Every callback it fires from here on is ignored by is_current().
        SAL_FUNC_INFO("retiring previous connection [@%p]", old.get());
#ifdef _SAL_TIME_H
        if (SAL::MSocketQueueManager::IsOnQueueThread())
        {
            old->terminate(lib::error_code());
        }
        else
        {
            // Shared so a wait that times out leaves nothing dangling for the task.
            std::shared_ptr<SAL::Event> done = std::make_shared<SAL::Event>(false);
            SAL::MSocketQueueManager::Post([old, done]() {
                old->terminate(lib::error_code());
                done->signal();
            });
            if (!done->wait(5000))
            {
                SAL_FUNC_ERROR("retire_current_connection: timed out waiting for socket-queue terminate");
            }
        }
#else
        old->terminate(lib::error_code());
#endif
    }

    bool client_impl::is_current(connection_hdl const& con)
    {
        if (con.expired())
        {
            return false;
        }
        std::lock_guard<std::mutex> lk(m_con_strong_mutex);
        return !con.owner_before(m_current_con) && !m_current_con.owner_before(con);
    }

    void client_impl::force_close_impl()
    {
        // Must run on the socket-queue thread (see sync_close). A graceful
        // close (close_impl → m_client.close) only sends the close
        // frame and waits for the peer's reply before the transport is shut
        // down — that shutdown (which UNREGISTERS the socket from the socket
        // queue) would otherwise happen AFTER ~client_impl, leaving the
        // websocketpp connection alive in the loop with handlers bound to a
        // freed client_impl → use-after-free in on_close.
        //
        // connection::terminate() forces it now: async_shutdown unregisters the
        // socket synchronously, then handle_terminate fires on_close — all
        // while client_impl is still alive. terminate() is idempotent.
        //
        // Cancel every timer here, on the socket-queue thread: timer expiries
        // are posted to this thread bound to `this`, and a cancelled timer's
        // pending expiry drops itself. on_close only clears the ping timers,
        // and only when there is a connection to terminate.
        reset_timer(m_reconn_timer);
        clear_timers();

        // Prefer the strong ref (captured in connect_impl, survives on_close's
        // m_con.reset()). Take and clear it under the lock, then drop the lock
        // before terminate() — terminate can fire on_close synchronously and we
        // must not hold the lock across that callback. Fall back to the weak hdl.
        client_type::connection_ptr con;
        {
            std::lock_guard<std::mutex> lk(m_con_strong_mutex);
            con = m_con_strong;
            m_con_strong.reset();
        }
        if (!con && !m_con.expired())
        {
            lib::error_code ec;
            con = m_client.get_con_from_hdl(m_con, ec);
            if (ec) con.reset();
        }

        if (con)
        {
            SAL_FUNC_INFO("force_close_impl: terminating connection");
            con->terminate(lib::error_code());
        }
        else
        {
            SAL_FUNC_INFO("force_close_impl: no active connection");
        }
    }

    void client_impl::send_impl(shared_ptr<const string> const& payload_ptr,frame::opcode::value opcode)
    {
        if(m_con_state == con_opened)
        {
            lib::error_code ec;
            m_client.send(m_con,*payload_ptr,opcode,ec);
            if(ec)
            {
                cerr<<"Send failed,reason:"<< ec.message()<<endl;
            }
        }
        else
        {
            cerr<<"wrong state:"<< m_con_state <<endl;
        }
    }

    void client_impl::timeout_wait(const std::error_code &ec)
    {
        if(ec)
        {
            return;
        }
        // Outside con_opened there is nothing to time out: between reconnect attempts a
        // close here would only cancel the pending reconnect, and a closing connection
        // is bounded by websocketpp's close-handshake timeout.
        if (m_con_state != con_opened)
        {
            SAL_FUNC_INFO("Ignoring ping/pong timeout, connection not open (state %d)", (int)m_con_state.load());
            return;
        }
        if (m_protocol_version == ProtocolVersion3)
        {
            SAL_FUNC_ERROR("Pong timeout");
            close_impl(close::status::policy_violation,"Pong timeout");
        }
        else
        {
            SAL_FUNC_ERROR("Ping timeout");
            close_impl(close::status::policy_violation,"Ping timeout");
        }
    }

    void client_impl::timeout_send(const std::error_code &ec)
    {
        if(ec)
        {
            return;
        }

        SAL_FUNC_DEBUG("Send ping");
        packet p(packet::frame_ping);
        m_packet_mgr.encode(p, [&](bool /*isBin*/,shared_ptr<const string> payload)
        {
            this->send_impl(payload, frame::opcode::text);
        });

		update_timeout_timer();
    }

    void client_impl::timeout_reconnect(std::error_code const& ec)
    {
        if(ec)
        {
            return;
        }
        // close() sets con_closing before m_abort_retries: a close that lands before
        // the exchange makes it fail, and one that lands after it is caught by
        // on_open's con_closing check.
        con_state expected = con_closed;
        if(!m_abort_retries && m_con_state.compare_exchange_strong(expected, con_opening))
        {
            m_reconn_made++;
			m_reconn_made_time = time(0);
			this->sockets_invoke_void(&sio::socket::on_close);
            this->retire_current_connection();
            this->reset_states();
            SAL_FUNC_INFO("Reconnecting...");
            if(m_reconnecting_listener) m_reconnecting_listener();
            this->connect_impl(m_base_url,m_query_string);
        }
    }

    unsigned client_impl::next_delay() const
    {
        //no jitter, fixed power root.
        unsigned reconn_made = std::min<unsigned>(m_reconn_made,32);//protect the pow result to be too big.
        return static_cast<unsigned>(std::min<double>(m_reconn_delay * pow(1.5,reconn_made),m_reconn_delay_max));
    }

    socket::ptr client_impl::get_socket_locked(string const& nsp)
    {
        lock_guard<mutex> guard(m_socket_mutex);
        auto it = m_sockets.find(nsp);
        if(it != m_sockets.end())
        {
            return it->second;
        }
        else
        {
            return socket::ptr();
        }
    }

    void client_impl::sockets_invoke_void(void (sio::socket::*fn)(void))
    {
        map<const string,socket::ptr> socks;
        {
            lock_guard<mutex> guard(m_socket_mutex);
            socks.insert(m_sockets.begin(),m_sockets.end());
        }
        for (auto it = socks.begin(); it!=socks.end(); ++it) {
            ((*(it->second)).*fn)();
        }
    }

    void client_impl::on_fail(connection_hdl con)
    {
        if (!is_current(con)) {
            SAL_FUNC_INFO("Ignoring failure of a retired connection.");
            return;
        }
        if (m_con_state == con_closing) {
            SAL_FUNC_WARN("Connection failed while closing.");
            this->close();
            return;
        }

        m_con.reset();
        m_con_state = con_closed;
        this->sockets_invoke_void(&sio::socket::on_disconnect);
        SAL_FUNC_ERROR("Connection failed.");
        if(m_reconn_made<m_reconn_attempts && !m_abort_retries)
        {
            SAL_FUNC_WARN("Reconnect for attempt: %" PRId64, (int64_t)m_reconn_made);
            unsigned delay = this->next_delay();
            if(m_reconnect_listener) m_reconnect_listener(m_reconn_made,delay);
            update_timer(m_reconn_timer, delay, &client_impl::timeout_reconnect);
        }
        else
        {
            if(m_fail_listener)m_fail_listener();
        }
    }

    void client_impl::on_open(connection_hdl con)
    {
        if (!is_current(con)) {
            SAL_FUNC_WARN("Ignoring open of a retired connection.");
            return;
        }
        if (m_con_state == con_closing) {
            SAL_FUNC_WARN("Connection opened while closing.");
            this->close();
            return;
        }

        SAL_FUNC_INFO("Connected.");
        m_con_state = con_opened;
        m_con = con;
        {
            // Refresh the strong ref from the now-open handle (connect_impl
            // already captured it before the socket was registered). Same lock
            // as connect_impl/force_close_impl — see m_con_strong_mutex (ON-1531).
            lib::error_code __ec;
            client_type::connection_ptr strong = m_client.get_con_from_hdl(con, __ec);
            std::lock_guard<std::mutex> lk(m_con_strong_mutex);
            m_con_strong = strong;
        }
        m_reconn_made = 0;
        this->sockets_invoke_void(&sio::socket::on_open);
        this->socket("");
        if(m_open_listener)m_open_listener();
    }

    void client_impl::on_close(connection_hdl con)
    {
        // A retired connection closing late must not tear down the state, timers and
        // namespace sockets of the connection that replaced it.
        if (!is_current(con)) {
            SAL_FUNC_INFO("Ignoring close of a retired connection.");
            return;
        }
        SAL_FUNC_INFO("Client Disconnected.");
        con_state m_con_state_was;
        {
            // See ping_timers_allowed.
            std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
            m_con_state_was = m_con_state.exchange(con_closed);
            this->clear_timers();
        }
        lib::error_code ec;
        close::status::value code = close::status::normal;
        client_type::connection_ptr conn_ptr  = m_client.get_con_from_hdl(con, ec);
        if (ec) {
            SAL_FUNC_WARN("OnClose get conn failed ec: %d / %s", (int)ec.value(), ec.message().c_str());
        }
        else
        {
            code = conn_ptr->get_local_close_code();
        }

        m_con.reset();
        client::close_reason reason;

        // If we initiated the close, no matter what the close status was,
        // we'll consider it a normal close. (When using TLS, we can
        // sometimes get a TLS Short Read error when closing.)
        if(code == close::status::normal || m_con_state_was == con_closing)
        {
            this->sockets_invoke_void(&sio::socket::on_disconnect);
            reason = client::close_reason_normal;
        }
        else
        {
            this->sockets_invoke_void(&sio::socket::on_disconnect);
            if(m_reconn_made<m_reconn_attempts && !m_abort_retries)
            {
                SAL_FUNC_WARN("Reconnect for attempt: %d", m_reconn_made);
                unsigned delay = this->next_delay();
                if(m_reconnect_listener) m_reconnect_listener(m_reconn_made,delay);
                update_timer(m_reconn_timer, delay, &client_impl::timeout_reconnect);
                return;
            }
            reason = client::close_reason_drop;
        }

        if(m_close_listener)
        {
            m_close_listener(reason);
        }
    }

    void client_impl::on_message(connection_hdl con, client_type::message_ptr msg)
    {
        // The packet manager and namespace map are shared, so a frame from a retired
        // connection (e.g. the router kicking its namespace socket) would act on the
        // current connection's namespaces.
        if (!is_current(con)) {
            SAL_FUNC_WARN("Dropping a frame from a retired connection.");
            return;
        }
        // Parse the incoming message according to socket.IO rules
        m_packet_mgr.put_payload(msg->get_payload());
    }

    void client_impl::on_handshake(message::ptr const& message)
    {
        if(message && message->get_flag() == message::flag_object)
        {
            const object_message* obj_ptr =static_cast<object_message*>(message.get());
            const map<string,message::ptr>* values = &(obj_ptr->get_map());
            auto it = values->find("sid");
            if (it!= values->end()) {
                m_sid = static_pointer_cast<string_message>(it->second)->get_string();
            }
            else
            {
                goto failed;
            }
            it = values->find("pingInterval");
            if (it!= values->end()&&it->second->get_flag() == message::flag_integer) {
                m_ping_interval = (unsigned)static_pointer_cast<int_message>(it->second)->get_int();
            }
            else
            {
                m_ping_interval = 25000;
            }

            it = values->find("pingTimeout");
            if (it!=values->end()&&it->second->get_flag() == message::flag_integer) {
                m_ping_timeout = (unsigned) static_pointer_cast<int_message>(it->second)->get_int();
            }
            else
            {
                m_ping_timeout = 60000;
            }

            // Start ping timeout
            switch (m_protocol_version)
            {
                case ProtocolVersion3:
                    // in protocol v3, the client sends a ping, and the server answers with a pong
                    update_send_timer();
                    break;
                case ProtocolVersion4: //
                    // in protocol v4, the server sends a ping, and the client answers with a pong
                    update_timeout_timer();
                    break;
            }

            return;
        }
failed:
        //just close it.
        // m_client.get_io_service().dispatch(std::bind(&client_impl::close_impl, this,close::status::policy_violation,"Handshake error"));
        close_impl(close::status::policy_violation,"Handshake error");
    }

    void client_impl::on_ping()
    {
		if (m_protocol_version < ProtocolVersion4)
        {
            // in protocol v4, the server sends a ping, and the client answers with a pong
            SAL_FUNC_DEBUG("Got ping");// don't return;
        }
        // Reply with pong packet.
        packet p(packet::frame_pong);
        m_packet_mgr.encode(p, [&](bool /*isBin*/,shared_ptr<const string> payload)
        {
            this->send_impl(payload, frame::opcode::text);
        });

        // Reset the ping timeout.
        update_timeout_timer();
    }

    void client_impl::on_pong()
    {
        SAL_FUNC_DEBUG("Got pong");

        // Clear the waiting-for-ping timer
        reset_timer(m_timeout_timer);

        // Reset the ping timeout.
        update_send_timer();
    }

    void client_impl::on_decode(packet const& p)
    {
        switch(p.get_frame())
        {
        case packet::frame_message:
        {
            socket::ptr so_ptr = get_socket_locked(p.get_nsp());
			if (p.get_type() == packet::type_disconnect && std::difftime(std::time(0), m_reconn_made_time) < 10)
            {
              SAL_FUNC_DEBUG("inc m_reconn_made");
              ++ m_reconn_made;
            }
            if(so_ptr)so_ptr->on_message_packet(p);else SAL_FUNC_VERBOSE("notfound");
            break;
        }
        case packet::frame_open:
            this->on_handshake(p.get_message());
            break;
        case packet::frame_close:
            //FIXME how to deal?
            this->close_impl(close::status::abnormal_close, "End by server");
            break;
        case packet::frame_ping:
            this->on_ping();
            break;
        case packet::frame_pong:
            this->on_pong();
            break;

        default:
			SAL_FUNC_VERBOSE("unknown");
            break;
        }
    }

    void client_impl::on_encode(bool isBinary,shared_ptr<const string> const& payload)
    {
        SAL_FUNC_VERBOSE("encoded payload length: %ld", (long) payload->length());
        // m_client.get_io_service().dispatch(std::bind(&client_impl::send_impl,this,payload,isBinary?frame::opcode::binary:frame::opcode::text));
        const frame::opcode::value opcode = isBinary ? frame::opcode::binary : frame::opcode::text;
#ifdef _SAL_TIME_H
        // emit() may come from any thread; the transport is driven by the socket-queue thread.
        run_on_queue([this, payload, opcode]() { this->send_impl(payload, opcode); });
#else
        send_impl(payload, opcode);
#endif
    }

    void client_impl::clear_timers()
    {
        SAL_FUNC_INFO("clear timers");
        std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
        reset_timer(m_timeout_timer);
        reset_timer(m_send_timer);
    }

	void client_impl::reset_timer(TIMER &timer) {
        // m_timer_mutex is recursive so callers that already hold it
        // (clear_timers, update_timer) can re-enter this helper. With the lock
        // held this can't race a concurrent reset/update on the same field.
        // The handle cancels the timer and drops our ref; the token itself is
        // freed only once TimerManager is done with it (see timer_cb).
        std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
        timer.reset();
	}

    bool client_impl::ping_timers_allowed() const
    {
        // A send that fails runs on_close (on this thread or another) and clears these
        // timers; a caller that re-arms right after its send would leave a stale
        // timeout firing mid-reconnect, or against the connection that replaced this
        // one. Callers hold m_timer_mutex from this check through the arming, and
        // on_close flips the state under the same lock.
        if (m_con_state == con_opened) return true;
        SAL_FUNC_DEBUG("Not arming ping timers, connection not open (state %d)", (int)m_con_state.load());
        return false;
    }

    void client_impl::update_send_timer() {
        if (m_protocol_version > ProtocolVersion3) { // both can come here but only v3
            // in protocol v3, the client sends a ping, and the server answers with a pong
            return;
        }
        std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
        if (!ping_timers_allowed()) return;
        SAL_FUNC_DEBUG("Set m_send_timer %" PRId64, (int64_t)m_ping_interval);
		update_timer(m_send_timer, m_ping_interval, &client_impl::timeout_send );
    }

    void client_impl::update_timeout_timer() {// both can come here
        std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
        if (!ping_timers_allowed()) return;
        int64_t _timeout = m_ping_timeout + (m_protocol_version > ProtocolVersion3 ? m_ping_interval : 0);
        SAL_FUNC_DEBUG("Set m_timeout_timer %" PRId64, _timeout);
        update_timer(m_timeout_timer, _timeout, &client_impl::timeout_wait);
    }

	void client_impl::update_timer(TIMER &timer, int timeout, func_ptr name)
	{
#ifdef _SAL_TIME_H
		std::lock_guard<std::recursive_mutex> lk(m_timer_mutex);
		reset_timer(timer);
		timer.reset(SAL::timer_cb::set_timer(timeout, std::bind(name, this, std::placeholders::_1)));
#else
		if (!timer) {
			timer = std::unique_ptr<asio::steady_timer>(new asio::steady_timer(get_io_service()));
		}

		asio::error_code ec;
		timer->expires_from_now(std::chrono::milliseconds(timeout), ec);
		timer->async_wait(std::bind(name, this, std::placeholders::_1));
#endif

	}

#ifdef _SAL_TIME_H
    void client_impl::run_on_queue(const std::function<void()> &task)
    {
        std::shared_ptr<std::atomic<bool>> alive = m_alive;
        SAL::MSocketQueueManager::Post([alive, task]() {
            if (*alive)
                task();
        });
    }
#endif

    void client_impl::reset_states()
    {
        m_client.reset();
        m_sid.clear();
        m_packet_mgr.reset();
    }

#if SIO_TLS
    client_impl::context_ptr client_impl::on_tls_init(connection_hdl conn)
    {
        context_ptr ctx = context_ptr(new  asio::ssl::context(asio::ssl::context::tls));
        asio::error_code ec;
        ctx->set_options(asio::ssl::context::default_workarounds |
                         asio::ssl::context::no_tlsv1 |
                         asio::ssl::context::no_tlsv1_1 |
                         asio::ssl::context::single_dh_use,ec);
        if(ec)
        {
            cerr<<"Init tls failed,reason:"<< ec.message()<<endl;
        }

        return ctx;
    }
#endif

    std::string client_impl::encode_query_string(const std::string &query){
        ostringstream ss;
        ss << std::hex;
        // Percent-encode (RFC3986) non-alphanumeric characters.
        for(const char c : query){
            if((c >= 'a' && c <= 'z') || (c>= 'A' && c<= 'Z') || (c >= '0' && c<= '9')){
                ss << c;
            } else {
                ss << '%' << std::uppercase << std::setw(2) << int((unsigned char) c) << std::nouppercase;
            }
        }
        ss << std::dec;
        return ss.str();
    }
}
