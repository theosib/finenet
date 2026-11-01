#include "finenet/connection.hpp"

#include "finenet/wake_signal.hpp"
#include "stream_transport.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <mutex>

namespace finenet {

// ============================================================================
// ChannelRouter
// ============================================================================

ChannelRouter ChannelRouter::createStandardRouter() {
    ChannelRouter router;

    // Channel 0 (Control)
    router.assign(MessageType::ClientHello, Channels::Control.id);
    router.assign(MessageType::ServerHello, Channels::Control.id);
    router.assign(MessageType::KeepAlive, Channels::Control.id);
    router.assign(MessageType::Disconnect, Channels::Control.id);
    router.assign(MessageType::ServerConfig, Channels::Control.id);
    router.assign(MessageType::DimensionTransition, Channels::Control.id);
    router.assign(MessageType::DimensionReady, Channels::Control.id);

    // Channel 1 (Input)
    router.assign(MessageType::PlayerInput, Channels::Input.id);

    // Channel 2 (Actions)
    router.assign(MessageType::PlayerAction, Channels::Actions.id);
    router.assign(MessageType::ActionResult, Channels::Actions.id);
    router.assign(MessageType::PlayerCorrection, Channels::Actions.id);
    router.assign(MessageType::BlockCorrection, Channels::Actions.id);
    router.assign(MessageType::VehicleMount, Channels::Actions.id);
    router.assign(MessageType::MinigameAction, Channels::Actions.id);
    router.assign(MessageType::ClaimEvent, Channels::Actions.id);
    router.assign(MessageType::TradeAction, Channels::Actions.id);
    router.assign(MessageType::RespawnRequest, Channels::Actions.id);

    // Channel 3 (Entities)
    router.assign(MessageType::EntityPosition, Channels::Entities.id);
    router.assign(MessageType::EntityBatch, Channels::Entities.id);
    router.assign(MessageType::EntitySpawn, Channels::Entities.id);
    router.assign(MessageType::EntityDespawn, Channels::Entities.id);
    router.assign(MessageType::EntityAnimation, Channels::Entities.id);
    router.assign(MessageType::InputAck, Channels::Entities.id);
    router.assign(MessageType::DamageEvent, Channels::Entities.id);
    router.assign(MessageType::EntityBatchSpawn, Channels::Entities.id);
    router.assign(MessageType::EntityBatchDespawn, Channels::Entities.id);
    router.assign(MessageType::VehicleState, Channels::Entities.id);
    router.assign(MessageType::BossState, Channels::Entities.id);
    router.assign(MessageType::GameEvent, Channels::Entities.id);

    // Channel 4 (Effects)
    router.assign(MessageType::SoundEvent, Channels::Effects.id);
    router.assign(MessageType::EffectTrigger, Channels::Effects.id);
    router.assign(MessageType::ParticleSpawn, Channels::Effects.id);

    // Channel 5 (World)
    router.assign(MessageType::ChunkData, Channels::World.id);
    router.assign(MessageType::ChunkUnload, Channels::World.id);
    router.assign(MessageType::BlockChange, Channels::World.id);
    router.assign(MessageType::LightUpdate, Channels::World.id);
    router.assign(MessageType::WorldTimeSync, Channels::World.id);
    router.assign(MessageType::CollapseEvent, Channels::World.id);
    router.assign(MessageType::FluidUpdate, Channels::World.id);
    router.assign(MessageType::WeatherUpdate, Channels::World.id);
    router.assign(MessageType::EnvironmentEvent, Channels::World.id);
    router.assign(MessageType::SignalBatch, Channels::World.id);
    router.assign(MessageType::MachineState, Channels::World.id);
    router.assign(MessageType::PowerGridState, Channels::World.id);
    router.assign(MessageType::BeltSegmentState, Channels::World.id);
    router.assign(MessageType::MapData, Channels::World.id);

    // Channel 6 (UI)
    router.assign(MessageType::OpenUI, Channels::UI.id);
    router.assign(MessageType::CloseUI, Channels::UI.id);
    router.assign(MessageType::UIUpdate, Channels::UI.id);
    router.assign(MessageType::UIAction, Channels::UI.id);
    router.assign(MessageType::InventoryUpdate, Channels::UI.id);
    router.assign(MessageType::VitalStats, Channels::UI.id);
    router.assign(MessageType::StatusEffect, Channels::UI.id);
    router.assign(MessageType::QuestUpdate, Channels::UI.id);
    router.assign(MessageType::NPCInteraction, Channels::UI.id);
    router.assign(MessageType::MerchantInventory, Channels::UI.id);
    router.assign(MessageType::CraftingState, Channels::UI.id);
    router.assign(MessageType::WaypointAction, Channels::UI.id);
    router.assign(MessageType::PartyUpdate, Channels::UI.id);

    // Channel 7 (Assets)
    router.assign(MessageType::AssetRequest, Channels::Assets.id);
    router.assign(MessageType::AssetResponse, Channels::Assets.id);
    router.assign(MessageType::BlockAppearanceList, Channels::Assets.id);
    router.assign(MessageType::ItemAppearanceList, Channels::Assets.id);
    router.assign(MessageType::EntityModelDef, Channels::Assets.id);
    router.assign(MessageType::TextureAtlas, Channels::Assets.id);
    router.assign(MessageType::AudioSample, Channels::Assets.id);
    router.assign(MessageType::UIDefinition, Channels::Assets.id);
    router.assign(MessageType::ClientRuleDef, Channels::Assets.id);
    router.assign(MessageType::NameRegistrySync, Channels::Assets.id);
    router.assign(MessageType::BiomeDefinitions, Channels::Assets.id);
    router.assign(MessageType::RecipeList, Channels::Assets.id);
    router.assign(MessageType::LootTableDefs, Channels::Assets.id);
    router.assign(MessageType::ScriptBundle, Channels::Assets.id);

    // Channel 8 (Chat)
    router.assign(MessageType::ChatMessage, Channels::Chat.id);

    return router;
}

// ============================================================================
// Connection::Impl
// ============================================================================

struct Connection::Impl {
    ConnectionConfig config;
    TransportType transportType = TransportType::Local;

    mutable std::mutex mutex;
    ConnectionState state = ConnectionState::Disconnected;
    std::string disconnectReason;

    std::weak_ptr<Impl> peer;

    // Set on a network connection, in place of peer
    std::shared_ptr<detail::StreamTransport> transport;

    // Delivered but not yet handed to a handler: for a local peer, that's what a sender has pending
    std::atomic<size_t> undispatchedBytes{0};
    std::array<std::atomic<size_t>, 256> undispatchedChannelBytes{};

    // Per-channel inbound sequence numbers, for a transport with none on the wire
    std::array<uint32_t, 256> nextInboundSeq{};

    // Channel configurations indexed by ChannelId
    std::unordered_map<ChannelId, ChannelConfig> channelConfigs;

    // Per-channel outbound sequence numbers
    std::array<uint32_t, 256> nextOutboundSeq{};

    // Inbound queues (per-channel and unified)
    mutable std::mutex queueMapMutex;
    std::unordered_map<ChannelId, std::unique_ptr<Queue<Message>>> channelQueues;
    Queue<Message> unifiedInboundQueue;
    std::atomic<bool> unifiedQueueAccessed{false};

    // Arrival order tracking for deterministic tie-breaking across channels of equal priority
    struct ArrivalKey {
        ChannelId channel;
        uint32_t seq;
        bool operator==(const ArrivalKey& other) const noexcept {
            return channel == other.channel && seq == other.seq;
        }
    };
    struct ArrivalKeyHash {
        size_t operator()(const ArrivalKey& k) const noexcept {
            return (static_cast<size_t>(k.channel) << 32) ^ static_cast<size_t>(k.seq);
        }
    };
    std::unordered_map<ArrivalKey, uint64_t, ArrivalKeyHash> arrivalOrders;
    uint64_t nextArrivalOrder = 0;

    // Handlers
    std::unordered_map<uint32_t, MessageHandler> messageHandlers;
    std::unordered_map<ChannelId, MessageHandler> channelHandlers;
    MessageHandler fallbackHandler;
    StateHandler stateHandler;

    // Statistics
    ConnectionStats stats;

    explicit Impl(const ConnectionConfig& cfg)
        : config(cfg) {
        // Seed with standard channels first so any standard channel works even if custom channels were passed
        for (const auto& stdCh : Channels::defaultChannels()) {
            channelConfigs[stdCh.id] = stdCh;
        }
        // Apply user-configured channels (overriding or adding)
        for (const auto& ch : config.channels) {
            channelConfigs[ch.id] = ch;
        }
    }

    ChannelConfig getChannelConfig(ChannelId channel) const {
        auto it = channelConfigs.find(channel);
        if (it != channelConfigs.end()) {
            return it->second;
        }
        ChannelConfig fallback;
        fallback.id = channel;
        return fallback;
    }

    Queue<Message>& getOrCreateChannelQueue(ChannelId channel) {
        std::lock_guard<std::mutex> qlock(queueMapMutex);
        auto it = channelQueues.find(channel);
        if (it == channelQueues.end()) {
            auto q = std::make_unique<Queue<Message>>();
            auto [emplacedIt, _] = channelQueues.emplace(channel, std::move(q));
            return *emplacedIt->second;
        }
        return *it->second;
    }

    /// Put a message where update() and the queues will find it. Takes this->mutex.
    SendResult deliverInbound(Message msg, size_t wireSize, uint32_t* droppedOut) {
        std::lock_guard<std::mutex> lock(mutex);
        if (state != ConnectionState::Connected) {
            return SendResult::NotConnected;
        }

        ChannelId channel = msg.channel();
        const ChannelConfig chCfg = getChannelConfig(channel);
        Queue<Message>& chQueue = getOrCreateChannelQueue(channel);

        uint32_t droppedCount = 0;
        if (chCfg.reliability == Reliability::Unreliable && chCfg.maxQueuedMessages > 0) {
            while (chQueue.size() >= chCfg.maxQueuedMessages) {
                auto dropped = chQueue.tryPop();
                if (dropped.has_value()) {
                    removeDroppedFromUnifiedQueue(channel, dropped->sequenceNumber());
                    arrivalOrders.erase(ArrivalKey{channel, dropped->sequenceNumber()});
                    undispatchedBytes -= dropped->size();
                    undispatchedChannelBytes[channel] -= dropped->size();
                    ++droppedCount;
                } else {
                    break;
                }
            }
        } else if (chQueue.isFull()) {
            return SendResult::ChannelFull;
        }

        if (droppedCount > 0) {
            stats.perChannel[channel].droppedMessages += droppedCount;
            if (droppedOut) *droppedOut = droppedCount;
        }

        const uint64_t arrivalId = nextArrivalOrder++;
        arrivalOrders[ArrivalKey{channel, msg.sequenceNumber()}] = arrivalId;

        stats.bytesReceived += wireSize;
        stats.packetsReceived += 1;
        stats.perChannel[channel].messagesReceived += 1;
        stats.perChannel[channel].bytesReceived += wireSize;
        undispatchedBytes += msg.size();
        undispatchedChannelBytes[channel] += msg.size();

        Message copyForUnified = msg;
        chQueue.push(std::move(msg));
        unifiedInboundQueue.push(std::move(copyForUnified));
        return SendResult::Ok;
    }

    void setDisconnected(std::string reason) {
        StateHandler handler;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (state == ConnectionState::Disconnected) return;
            state = ConnectionState::Disconnected;
            disconnectReason = std::move(reason);
            handler = stateHandler;
        }
        if (handler) handler(ConnectionState::Disconnected);
    }

    /// Give this connection a socket transport. Its handlers only hold a weak reference: the
    /// connection owns the transport, and its thread is joined before either goes away.
    static void attachTransport(const std::shared_ptr<Impl>& impl) {
        std::weak_ptr<Impl> weak = impl;
        detail::StreamTransport::Handlers handlers;
        handlers.connected = [weak] {
            auto self = weak.lock();
            if (!self) return;
            StateHandler handler;
            {
                std::lock_guard<std::mutex> lock(self->mutex);
                if (self->state != ConnectionState::Connecting) return;
                self->state = ConnectionState::Connected;
                handler = self->stateHandler;
            }
            if (handler) handler(ConnectionState::Connected);
        };
        handlers.message = [weak](Message message) {
            auto self = weak.lock();
            if (!self) return;
            ChannelId channel = message.channel();
            message.setPriority(self->getChannelConfig(channel).defaultPriority);
            {
                std::lock_guard<std::mutex> lock(self->mutex);
                message.setSequenceNumber(self->nextInboundSeq[channel]++);
            }
            size_t size = message.size();
            self->deliverInbound(std::move(message), size, nullptr);
        };
        handlers.closed = [weak](std::string reason) {
            if (auto self = weak.lock()) self->setDisconnected(std::move(reason));
        };
        impl->transportType = TransportType::Tcp;
        impl->transport = std::make_shared<detail::StreamTransport>(impl->config, std::move(handlers));
    }

    void removeDroppedFromUnifiedQueue(ChannelId channel, uint32_t droppedSeq) {
        auto all = unifiedInboundQueue.drainAll();
        bool removed = false;
        std::vector<Message> kept;
        kept.reserve(all.size());
        for (auto& m : all) {
            if (!removed && m.channel() == channel && m.sequenceNumber() == droppedSeq) {
                removed = true;
                continue;
            }
            kept.push_back(std::move(m));
        }
        if (!kept.empty()) {
            unifiedInboundQueue.pushBatch(std::move(kept));
        }
    }
};

// ============================================================================
// Connection Implementation
// ============================================================================

Connection::Connection(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Connection::~Connection() {
    if (impl_) {
        disconnect("Connection destroyed");
        // Even if it had already closed: the I/O thread must be gone before this object is
        if (impl_->transport) impl_->transport->close("", false);
    }
}

std::pair<std::unique_ptr<Connection>, std::unique_ptr<Connection>>
Connection::createLocalPair(const ConnectionConfig& config) {
    auto clientImpl = std::make_shared<Impl>(config);
    auto serverImpl = std::make_shared<Impl>(config);

    clientImpl->state = ConnectionState::Connected;
    serverImpl->state = ConnectionState::Connected;

    clientImpl->peer = serverImpl;
    serverImpl->peer = clientImpl;

    auto client = std::unique_ptr<Connection>(new Connection(std::move(clientImpl)));
    auto server = std::unique_ptr<Connection>(new Connection(std::move(serverImpl)));
    return {std::move(client), std::move(server)};
}

std::unique_ptr<Connection> Connection::connect(const std::string& host, uint16_t port,
                                                const ConnectionConfig& config) {
    auto impl = std::make_shared<Impl>(config);
    impl->state = ConnectionState::Connecting;
    Impl::attachTransport(impl);
    impl->transport->startConnect(host, port);
    return std::unique_ptr<Connection>(new Connection(std::move(impl)));
}

std::unique_ptr<Connection> Connection::fromAccepted(int fd, std::string peerAddress,
                                                     const ConnectionConfig& config) {
    auto impl = std::make_shared<Impl>(config);
    impl->state = ConnectionState::Connecting;
    Impl::attachTransport(impl);
    impl->transport->startAccepted(fd, std::move(peerAddress));
    return std::unique_ptr<Connection>(new Connection(std::move(impl)));
}

ConnectionState Connection::state() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->state;
}

bool Connection::isConnected() const {
    return state() == ConnectionState::Connected;
}

std::string Connection::disconnectReason() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->disconnectReason;
}

TransportType Connection::transportType() const noexcept {
    return impl_->transportType;
}

std::string Connection::peerAddress() const {
    return impl_->transport ? impl_->transport->peerAddress() : std::string();
}

const ConnectionConfig& Connection::config() const noexcept {
    return impl_->config;
}

size_t Connection::pendingBytes() const {
    if (impl_->transport) return impl_->transport->pendingBytes();
    std::shared_ptr<Impl> peer;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        peer = impl_->peer.lock();
    }
    return peer ? peer->undispatchedBytes.load() : 0;
}

size_t Connection::pendingBytes(ChannelId channel) const {
    if (impl_->transport) return impl_->transport->pendingBytes(channel);
    std::shared_ptr<Impl> peer;
    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        peer = impl_->peer.lock();
    }
    return peer ? peer->undispatchedChannelBytes[channel].load() : 0;
}

std::optional<ChannelConfig> Connection::channelConfig(ChannelId channel) const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    auto it = impl_->channelConfigs.find(channel);
    if (it != impl_->channelConfigs.end()) {
        return it->second;
    }
    return std::nullopt;
}

SendResult Connection::send(ChannelId channel, Message msg) {
    return sendInternal(channel, std::move(msg), std::nullopt);
}

SendResult Connection::send(ChannelId channel, Message msg, Priority overridePriority) {
    return sendInternal(channel, std::move(msg), overridePriority);
}

SendResult Connection::sendInternal(
    ChannelId channel,
    Message msg,
    std::optional<Priority> overridePriority) {
    std::shared_ptr<Impl> peerImpl;
    ChannelConfig chCfg;
    uint32_t seqNum = 0;
    bool debugSerialize = false;

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->state != ConnectionState::Connected) {
            return SendResult::NotConnected;
        }
        if (!impl_->transport) {
            peerImpl = impl_->peer.lock();
            if (!peerImpl) {
                return SendResult::NotConnected;
            }
        }

        chCfg = impl_->getChannelConfig(channel);
        if (chCfg.maxMessageSize > 0 && msg.payloadSize() > chCfg.maxMessageSize) {
            return SendResult::MessageTooLarge;
        }

        seqNum = impl_->nextOutboundSeq[channel]++;
        debugSerialize = impl_->config.debugSerialization;
    }

    Priority effectivePriority = chCfg.defaultPriority;
    if (overridePriority.has_value() && chCfg.allowPriorityOverride) {
        effectivePriority = *overridePriority;
    }

    msg.setChannel(channel);
    msg.setPriority(effectivePriority);
    msg.setSequenceNumber(seqNum);
    msg.setReliable(chCfg.reliability != Reliability::Unreliable);
    msg.setOrdered(chCfg.ordered || chCfg.reliability == Reliability::ReliableOrdered);

    const size_t msgWireSize = msg.size();

    uint32_t droppedCount = 0;
    if (impl_->transport) {
        SendResult result = impl_->transport->send(msg, chCfg, effectivePriority);
        if (result != SendResult::Ok) return result;
    } else {
        if (debugSerialize) {
            const auto wireBytes = msg.toWireBytes();
            auto decoded = Message::fromWireBytes(wireBytes);
            if (!decoded.has_value()) {
                return SendResult::NotConnected;
            }
            msg = std::move(*decoded);
        }

        // Deliver to peer
        SendResult result = peerImpl->deliverInbound(std::move(msg), msgWireSize, &droppedCount);
        if (result != SendResult::Ok) return result;
    }

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        impl_->stats.bytesSent += msgWireSize;
        impl_->stats.packetsSent += 1;
        impl_->stats.perChannel[channel].messagesSent += 1;
        impl_->stats.perChannel[channel].bytesSent += msgWireSize;
        if (droppedCount > 0) {
            impl_->stats.perChannel[channel].droppedMessages += droppedCount;
        }
    }

    return SendResult::Ok;
}

void Connection::onMessage(MessageType type, MessageHandler handler) {
    onMessage(static_cast<uint32_t>(type), std::move(handler));
}

void Connection::onMessage(uint32_t typeId, MessageHandler handler) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (handler) {
        impl_->messageHandlers[typeId] = std::move(handler);
    } else {
        impl_->messageHandlers.erase(typeId);
    }
}

bool Connection::hasMessageHandler(MessageType type) const {
    return hasMessageHandler(static_cast<uint32_t>(type));
}

bool Connection::hasMessageHandler(uint32_t typeId) const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    return impl_->messageHandlers.find(typeId) != impl_->messageHandlers.end();
}

void Connection::onChannel(ChannelId channel, MessageHandler handler) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (handler) {
        impl_->channelHandlers[channel] = std::move(handler);
    } else {
        impl_->channelHandlers.erase(channel);
    }
}

void Connection::onFallback(MessageHandler handler) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->fallbackHandler = std::move(handler);
}

void Connection::onStateChange(StateHandler handler) {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    impl_->stateHandler = std::move(handler);
}

Queue<Message>& Connection::inboundQueue() {
    impl_->unifiedQueueAccessed.store(true, std::memory_order_relaxed);
    return impl_->unifiedInboundQueue;
}

Queue<Message>& Connection::inboundQueue(ChannelId channel) {
    return impl_->getOrCreateChannelQueue(channel);
}

void Connection::setWakeSignal(WakeSignal* signal) {
    impl_->unifiedInboundQueue.attach(signal);
}

void Connection::update() {
    struct ScheduledMessage {
        uint8_t priorityRank;
        uint64_t arrivalOrder;
        Message msg;
    };

    std::vector<ScheduledMessage> toDispatch;
    std::unordered_map<uint32_t, MessageHandler> msgHandlers;
    std::unordered_map<ChannelId, MessageHandler> chHandlers;
    MessageHandler fallback;

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        std::lock_guard<std::mutex> qlock(impl_->queueMapMutex);

        size_t totalInChannelQueues = 0;
        for (const auto& [chId, qPtr] : impl_->channelQueues) {
            totalInChannelQueues += qPtr->size();
        }

        const size_t unifiedCount = impl_->unifiedInboundQueue.size();

        if (impl_->unifiedQueueAccessed.load(std::memory_order_relaxed) &&
            unifiedCount < totalInChannelQueues) {
            // Caller popped directly from unifiedInboundQueue; drain remaining from it
            auto remaining = impl_->unifiedInboundQueue.drainAll();
            for (auto& [chId, qPtr] : impl_->channelQueues) {
                qPtr->clear();
            }
            toDispatch.reserve(remaining.size());
            for (auto& m : remaining) {
                uint64_t order = 0;
                auto it = impl_->arrivalOrders.find(
                    Impl::ArrivalKey{m.channel(), m.sequenceNumber()});
                if (it != impl_->arrivalOrders.end()) {
                    order = it->second;
                }
                toDispatch.push_back(ScheduledMessage{
                    static_cast<uint8_t>(m.priority()),
                    order,
                    std::move(m),
                });
            }
        } else {
            // Drain from per-channel queues and clear unifiedInboundQueue
            impl_->unifiedInboundQueue.clear();
            toDispatch.reserve(totalInChannelQueues);
            for (auto& [chId, qPtr] : impl_->channelQueues) {
                auto drained = qPtr->drainAll();
                for (auto& m : drained) {
                    uint64_t order = 0;
                    auto it = impl_->arrivalOrders.find(
                        Impl::ArrivalKey{m.channel(), m.sequenceNumber()});
                    if (it != impl_->arrivalOrders.end()) {
                        order = it->second;
                    }
                    toDispatch.push_back(ScheduledMessage{
                        static_cast<uint8_t>(m.priority()),
                        order,
                        std::move(m),
                    });
                }
            }
        }

        impl_->arrivalOrders.clear();
        msgHandlers = impl_->messageHandlers;
        chHandlers = impl_->channelHandlers;
        fallback = impl_->fallbackHandler;
    }

    if (toDispatch.empty()) {
        return;
    }

    for (const auto& item : toDispatch) {
        impl_->undispatchedBytes -= item.msg.size();
        impl_->undispatchedChannelBytes[item.msg.channel()] -= item.msg.size();
    }

    // Sort by Priority (Critical=0 first .. Bulk=4 last), breaking ties by arrivalOrder
    std::stable_sort(toDispatch.begin(), toDispatch.end(),
        [](const ScheduledMessage& a, const ScheduledMessage& b) {
            if (a.priorityRank != b.priorityRank) {
                return a.priorityRank < b.priorityRank;
            }
            return a.arrivalOrder < b.arrivalOrder;
        });

    // Dispatch according to §12.8 priority chain:
    // 1. onMessage(typeId)
    // 2. onChannel(channelId)
    // 3. fallbackHandler
    for (const auto& item : toDispatch) {
        const Message& msg = item.msg;
        auto mIt = msgHandlers.find(msg.typeId());
        if (mIt != msgHandlers.end() && mIt->second) {
            mIt->second(msg);
            continue;
        }

        auto cIt = chHandlers.find(msg.channel());
        if (cIt != chHandlers.end() && cIt->second) {
            cIt->second(msg);
            continue;
        }

        if (fallback) {
            fallback(msg);
        }
    }
}

void Connection::disconnect(std::string_view reason) {
    if (impl_->transport) {
        // First, so that send() stops being accepted before the state says it should
        bool wasOpen;
        {
            std::lock_guard<std::mutex> lock(impl_->mutex);
            wasOpen = impl_->state != ConnectionState::Disconnected;
            if (wasOpen) {
                impl_->state = ConnectionState::Disconnecting;
                impl_->disconnectReason = std::string(reason);
            }
        }
        if (wasOpen) {
            impl_->transport->close(reason, true);
            impl_->setDisconnected(std::string(reason));
        }
        return;
    }

    StateHandler selfStateHandler;
    StateHandler peerStateHandler;
    std::shared_ptr<Impl> peerImpl;

    {
        std::lock_guard<std::mutex> lock(impl_->mutex);
        if (impl_->state == ConnectionState::Disconnected) {
            return;
        }
        impl_->state = ConnectionState::Disconnected;
        impl_->disconnectReason = std::string(reason);
        selfStateHandler = impl_->stateHandler;
        peerImpl = impl_->peer.lock();
        impl_->peer.reset();
    }

    if (selfStateHandler) {
        selfStateHandler(ConnectionState::Disconnected);
    }

    if (peerImpl) {
        bool peerChanged = false;
        {
            std::lock_guard<std::mutex> peerLock(peerImpl->mutex);
            if (peerImpl->state != ConnectionState::Disconnected) {
                peerImpl->state = ConnectionState::Disconnected;
                peerImpl->disconnectReason = std::string(reason);
                peerStateHandler = peerImpl->stateHandler;
                peerImpl->peer.reset();
                peerChanged = true;
            }
        }
        if (peerChanged && peerStateHandler) {
            peerStateHandler(ConnectionState::Disconnected);
        }
    }
}

ConnectionStats Connection::stats() const {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    ConnectionStats out = impl_->stats;
    if (impl_->transport) out.rttMs = impl_->transport->rttMs();
    return out;
}

}  // namespace finenet
