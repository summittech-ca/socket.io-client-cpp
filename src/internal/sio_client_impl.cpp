//
//  sio_client_impl.cpp
//  SioChatDemo
//
//  Created by Melo Yao on 4/3/15.
//  Copyright (c) 2015 Melo Yao. All rights reserved.
//

#include "sio_client_impl.h"
#include <functional>
#include <sstream>
#include <chrono>
#include <mutex>
#include <cmath>
// Comment this out to disable handshake logging to stdout

#include "SAL/Log/Log.h"

namespace {
    LOGGER_NAME("socketio.client")
}

#if SIO_TLS
// If using Asio's SSL support, you will also need to add this #include.
// Source: http://think-async.com/Asio/asio-1.10.6/doc/asio/using.html
// #include <asio/ssl/impl/src.hpp>
#endif

using std::chrono::milliseconds;

SAL::TimerManagerPtr getTimerMgr();

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
        // Initialize the Asio transport policy
        // m_client.init_asio();

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

    // void client_impl::set_proxy_basic_auth(const std::string& uri, const std::string& username, const std::string& password)
    // {
    //     m_proxy_base_url = uri;
    //     m_proxy_basic_username = username;
    //     m_proxy_basic_password = password;
    // }

    void client_impl::connect(const string& uri, const map<string,string>& query, const map<string, string>& headers, const message::ptr& auth)
    {
        if(m_reconn_timer)
        {
            m_reconn_timer->cancel();
            m_reconn_timer.reset();
        }
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

        this->reset_states();
        m_abort_retries = false;
        // m_client.get_io_service().dispatch(std::bind(&client_impl::connect_impl,this,uri,m_query_string));
        this->connect_impl(uri, m_query_string);
        // m_network_thread.reset(new thread(std::bind(&client_impl::run_loop,this)));//uri lifecycle?

    }

    socket::ptr const& client_impl::socket(string const& nsp)
    {
        lock_guard<mutex> guard(m_socket_mutex);
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

        auto it = m_sockets.find(aux);
        if(it!= m_sockets.end())
        {
            return it->second;
        }
        else
        {
            pair<const string, socket::ptr> p(aux,shared_ptr<sio::socket>(new sio::socket(this,aux,m_auth)));
            return (m_sockets.insert(p).first)->second;
        }
    }

    void client_impl::close()
    {
        m_con_state = con_closing;
        m_abort_retries = true;
        this->sockets_invoke_void(&sio::socket::close);
        // m_client.get_io_service().dispatch(std::bind(&client_impl::close_impl, this,close::status::normal,"End by user"));
        close_impl(close::status::normal,"End by user");
    }

    void client_impl::sync_close()
    {
        m_con_state = con_closing;
        m_abort_retries = true;
        this->sockets_invoke_void(&sio::socket::close);
        // m_client.get_io_service().dispatch(std::bind(&client_impl::close_impl, this,close::status::normal,"End by user"));
        close_impl(close::status::normal,"End by user");
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

    // asio::io_service& client_impl::get_io_service()
    // {
    //     return m_client.get_io_service();
    // }

    void client_impl::on_socket_closed(string const& nsp)
    {
        if(m_socket_close_listener)m_socket_close_listener(nsp);
    }

    void client_impl::on_socket_opened(string const& nsp)
    {
        if(m_socket_open_listener)m_socket_open_listener(nsp);
    }

    /*************************private:*************************/
    // void client_impl::run_loop()
    // {

    //     m_client.run();
    //     m_client.reset();
    //     m_client.get_alog().write(websocketpp::log::alevel::devel,
    //                               "run loop end");
    // }

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

            // if (!m_proxy_base_url.empty()) {
            //     con->set_proxy(m_proxy_base_url, ec);
            //     if (ec) {
            //         m_client.get_alog().write(websocketpp::log::alevel::app,
            //                         "Set Proxy Error: " + ec.message());
            //         break;
            //     }
            //     if (!m_proxy_basic_username.empty()) {
            //         con->set_proxy_basic_auth(m_proxy_basic_username, m_proxy_basic_password, ec);
            //         if (ec) {
            //             m_client.get_alog().write(websocketpp::log::alevel::app,
            //                         "Set Proxy Basic Auth Error: " + ec.message());
            //             break;
            //         }
            //     }
            // }

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
        SAL_FUNC_INFO("Close by reason: %s", reason.c_str());
        if(m_reconn_timer)
        {
            m_reconn_timer->cancel();
            m_reconn_timer.reset();
        }
        if (m_con.expired())
        {
            cerr << "Error: No active session" << endl;
        }
        else
        {
            lib::error_code ec;
            m_client.close(m_con, code, reason, ec);
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
    }

    void client_impl::timeout_ping(const std::error_code &ec)
    {
        if(ec)
        {
            return;
        }
        SAL_FUNC_ERROR("Ping timeout");
        close_impl(close::status::policy_violation,"Ping timeout");
    }

    void client_impl::timeout_send_ping(const std::error_code &ec)
    {
        if(ec)
        {
            return;
        }

        SAL_FUNC_DEBUG("Send ping");
        packet p(packet::frame_ping);
        m_packet_mgr.encode(p, [&](bool /*isBin*/,shared_ptr<const string> payload)
        {
            this->m_client.send(this->m_con, *payload, frame::opcode::text);
        });

        update_wait_pong_timeout_timer();
    }

    void client_impl::timeout_wait_pong(const std::error_code &ec)
    {
        if(ec)
        {
            return;
        }
        SAL_FUNC_ERROR("Pong timeout");
        close_impl(close::status::policy_violation,"Pong timeout");
    }

    void client_impl::timeout_reconnect(std::error_code const& ec)
    {
        if(ec)
        {
            return;
        }
        if(m_con_state == con_closed)
        {
            m_con_state = con_opening;
            m_reconn_made++;
            this->reset_states();
            SAL_FUNC_INFO("Reconnecting...");
            if(m_reconnecting_listener) m_reconnecting_listener();
            // m_client.get_io_service().dispatch(std::bind(&client_impl::connect_impl,this,m_base_url,m_query_string));
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

    void client_impl::on_fail(connection_hdl)
    {
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
            m_reconn_timer.reset(SAL::timer_cb::set_timer(delay, std::bind(&client_impl::timeout_reconnect,this, std::placeholders::_1)));
            // m_reconn_timer.reset(new asio::steady_timer(m_client.get_io_service()));
            // asio::error_code ec;
            // m_reconn_timer->expires_from_now(milliseconds(delay), ec);
            // m_reconn_timer->async_wait(std::bind(&client_impl::timeout_reconnect,this, std::placeholders::_1));
        }
        else
        {
            if(m_fail_listener)m_fail_listener();
        }
    }

    void client_impl::on_open(connection_hdl con)
    {
        if (m_con_state == con_closing) {
            SAL_FUNC_WARN("Connection opened while closing.");
            this->close();
            return;
        }

        SAL_FUNC_INFO("Connected.");
        m_con_state = con_opened;
        m_con = con;
        m_reconn_made = 0;
        this->sockets_invoke_void(&sio::socket::on_open);
        this->socket("");
        if(m_open_listener)m_open_listener();
    }

    void client_impl::on_close(connection_hdl con)
    {
        SAL_FUNC_INFO("Client Disconnected.");
        con_state m_con_state_was = m_con_state;
        m_con_state = con_closed;
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
        this->clear_timers();
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
                m_reconn_timer.reset(SAL::timer_cb::set_timer(delay, std::bind(&client_impl::timeout_reconnect,this, std::placeholders::_1)));
                // m_reconn_timer.reset(new asio::steady_timer(m_client.get_io_service()));
                // asio::error_code ec;
                // m_reconn_timer->expires_from_now(milliseconds(delay), ec);
                // m_reconn_timer->async_wait(std::bind(&client_impl::timeout_reconnect,this, std::placeholders::_1));
                return;
            }
            reason = client::close_reason_drop;
        }

        if(m_close_listener)
        {
            m_close_listener(reason);
        }
    }

    void client_impl::on_message(connection_hdl, client_type::message_ptr msg)
    {
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
                    update_send_ping_timer();
                    break;
                case ProtocolVersion4: //
                    // in protocol v4, the server sends a ping, and the client answers with a pong
                    update_ping_timeout_timer();
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
        // Reply with pong packet.
        packet p(packet::frame_pong);
        m_packet_mgr.encode(p, [&](bool /*isBin*/,shared_ptr<const string> payload)
        {
            this->m_client.send(this->m_con, *payload, frame::opcode::text);
        });

        // Reset the ping timeout.
        update_ping_timeout_timer();
    }

    void client_impl::on_pong()
    {
        SAL_FUNC_DEBUG("Got pong");

        // Clear the waiting-for-ping timer
        if (m_wait_pong_timeout_timer)
        {
            m_wait_pong_timeout_timer->cancel();
            m_wait_pong_timeout_timer.reset();
        }

        // Reset the ping timeout.
        update_send_ping_timer();
    }

    void client_impl::on_decode(packet const& p)
    {
        switch(p.get_frame())
        {
        case packet::frame_message:
        {
            socket::ptr so_ptr = get_socket_locked(p.get_nsp());
            if(so_ptr)so_ptr->on_message_packet(p);
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
            break;
        }
    }

    void client_impl::on_encode(bool isBinary,shared_ptr<const string> const& payload)
    {
        SAL_FUNC_VERBOSE("encoded payload length: %ld", (long) payload->length());
        // m_client.get_io_service().dispatch(std::bind(&client_impl::send_impl,this,payload,isBinary?frame::opcode::binary:frame::opcode::text));
        send_impl(payload,isBinary?frame::opcode::binary:frame::opcode::text);
    }

    void client_impl::clear_timers()
    {
        SAL_FUNC_INFO("clear timers");
        std::error_code ec;
        if(m_ping_timeout_timer)
        {

            m_ping_timeout_timer->cancel(ec);
            m_ping_timeout_timer.reset();
        }
        if(m_send_ping_timer)
        {

            m_send_ping_timer->cancel(ec);
            m_send_ping_timer.reset();
        }
        if(m_wait_pong_timeout_timer)
        {

            m_wait_pong_timeout_timer->cancel(ec);
            m_wait_pong_timeout_timer.reset();
        }
    }

    void client_impl::update_ping_timeout_timer() {

        if (m_protocol_version < ProtocolVersion4)
        {
            // in protocol v4, the server sends a ping, and the client answers with a pong
            return;
        }
        if (m_ping_timeout_timer)
        {
            m_ping_timeout_timer->cancel();
            m_ping_timeout_timer.reset();
        }
        m_ping_timeout_timer.reset(SAL::timer_cb::set_timer(m_ping_interval + m_ping_timeout, std::bind(&client_impl::timeout_ping, this, std::placeholders::_1)));

        // if (!m_ping_timeout_timer) {
        //     m_ping_timeout_timer = std::unique_ptr<asio::steady_timer>(new asio::steady_timer(get_io_service()));
        // }

        // asio::error_code ec;
        // m_ping_timeout_timer->expires_from_now(milliseconds(m_ping_interval + m_ping_timeout), ec);
        // m_ping_timeout_timer->async_wait(std::bind(&client_impl::timeout_ping, this, std::placeholders::_1));
    }

    void client_impl::update_send_ping_timer() {

        if (m_protocol_version > ProtocolVersion3)
        {
            // in protocol v3, the client sends a ping, and the server answers with a pong
            return;
        }
        SAL_FUNC_DEBUG("Set send_ping_timer %" PRId64, (int64_t)m_ping_interval);
        if (m_send_ping_timer)
        {
            m_send_ping_timer->cancel();
            m_send_ping_timer.reset();
        }
        m_send_ping_timer.reset(SAL::timer_cb::set_timer(m_ping_interval, std::bind(&client_impl::timeout_send_ping, this, std::placeholders::_1)));
    }

    void client_impl::update_wait_pong_timeout_timer() {

        SAL_FUNC_DEBUG("Set wait_pong_timeout_timer %" PRId64, (int64_t)m_ping_timeout);
        if (m_wait_pong_timeout_timer)
        {
            m_wait_pong_timeout_timer->cancel();
            m_wait_pong_timeout_timer.reset();
        }
        m_wait_pong_timeout_timer.reset(SAL::timer_cb::set_timer(m_ping_timeout, std::bind(&client_impl::timeout_wait_pong, this, std::placeholders::_1)));
    }

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
