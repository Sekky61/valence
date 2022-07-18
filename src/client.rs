//! Connections to the server after logging in.

/// Contains the [`Event`] enum and related data types.
mod event;
use std::collections::{HashSet, VecDeque};
use std::iter::FusedIterator;
use std::time::Duration;

use bitfield_struct::bitfield;
pub use event::*;
use flume::{Receiver, Sender, TrySendError};
use rayon::iter::ParallelIterator;
use uuid::Uuid;
use vek::Vec3;

use crate::biome::Biome;
use crate::block_pos::BlockPos;
use crate::chunk_pos::ChunkPos;
use crate::config::Config;
use crate::dimension::DimensionId;
use crate::entity::types::Player;
use crate::entity::{velocity_to_packet_units, Entities, Entity, EntityId, EntityKind};
use crate::player_textures::SignedPlayerTextures;
use crate::protocol_inner::packets::play::c2s::{
    C2sPlayPacket, DiggingStatus, InteractKind, PlayerCommandId,
};
pub use crate::protocol_inner::packets::play::s2c::SetTitleAnimationTimes as TitleAnimationTimes;
use crate::protocol_inner::packets::play::s2c::{
    Animate, BiomeRegistry, BlockChangeAck, ChatType, ChatTypeChat, ChatTypeNarration,
    ChatTypeRegistry, ChatTypeRegistryEntry, ClearTitles, DimensionTypeRegistry,
    DimensionTypeRegistryEntry, Disconnect, EntityEvent, ForgetLevelChunk, GameEvent,
    GameEventReason, KeepAlive, Login, MoveEntityPosition, MoveEntityPositionAndRotation,
    MoveEntityRotation, PlayerPosition, PlayerPositionFlags, RegistryCodec, RemoveEntities,
    Respawn, RotateHead, S2cPlayPacket, SetChunkCacheCenter, SetChunkCacheRadius,
    SetEntityMetadata, SetEntityMotion, SetSubtitleText, SetTitleText, SpawnPosition, SystemChat,
    TeleportEntity, UpdateAttributes, UpdateAttributesProperty, ENTITY_EVENT_MAX_BOUND,
};
use crate::protocol_inner::{BoundedInt, ByteAngle, Nbt, RawBytes, VarInt};
use crate::server::{C2sPacketChannels, NewClientData, SharedServer};
use crate::slotmap::{Key, SlotMap};
use crate::text::Text;
use crate::util::{chunks_in_view_distance, is_chunk_in_view_distance};
use crate::world::{WorldId, Worlds};
use crate::{ident, Ticks, LIBRARY_NAMESPACE, STANDARD_TPS};

/// A container for all [`Client`]s on a [`Server`](crate::server::Server).
///
/// New clients are automatically inserted into this container but
/// are not automatically deleted. It is your responsibility to delete them once
/// they disconnect. This can be checked with [`Client::is_disconnected`].
pub struct Clients<C: Config> {
    sm: SlotMap<Client<C>>,
}

impl<C: Config> Clients<C> {
    pub(crate) fn new() -> Self {
        Self { sm: SlotMap::new() }
    }

    pub(crate) fn insert(&mut self, client: Client<C>) -> (ClientId, &mut Client<C>) {
        let (id, client) = self.sm.insert(client);
        (ClientId(id), client)
    }

    /// Removes a client from the server.
    ///
    /// If the given client ID is valid, `true` is returned and the client is
    /// deleted. Otherwise, `false` is returned and the function has no effect.
    pub fn delete(&mut self, client: ClientId) -> bool {
        self.sm.remove(client.0).is_some()
    }

    /// Deletes all clients from the server (as if by [`Self::delete`]) for
    /// which `f` returns `true`.
    ///
    /// All clients are visited in an unspecified order.
    pub fn retain(&mut self, mut f: impl FnMut(ClientId, &mut Client<C>) -> bool) {
        self.sm.retain(|k, v| f(ClientId(k), v))
    }

    /// Returns the number of clients on the server. This includes clients
    /// which may be disconnected.
    pub fn count(&self) -> usize {
        self.sm.len()
    }

    /// Returns a shared reference to the client with the given ID. If
    /// the ID is invalid, then `None` is returned.
    pub fn get(&self, client: ClientId) -> Option<&Client<C>> {
        self.sm.get(client.0)
    }

    /// Returns an exclusive reference to the client with the given ID. If the
    /// ID is invalid, then `None` is returned.
    pub fn get_mut(&mut self, client: ClientId) -> Option<&mut Client<C>> {
        self.sm.get_mut(client.0)
    }

    /// Returns an immutable iterator over all clients on the server in an
    /// unspecified order.
    pub fn iter(&self) -> impl FusedIterator<Item = (ClientId, &Client<C>)> + Clone + '_ {
        self.sm.iter().map(|(k, v)| (ClientId(k), v))
    }

    /// Returns a mutable iterator over all clients on the server in an
    /// unspecified order.
    pub fn iter_mut(&mut self) -> impl FusedIterator<Item = (ClientId, &mut Client<C>)> + '_ {
        self.sm.iter_mut().map(|(k, v)| (ClientId(k), v))
    }

    /// Returns a parallel immutable iterator over all clients on the server in
    /// an unspecified order.
    pub fn par_iter(&self) -> impl ParallelIterator<Item = (ClientId, &Client<C>)> + Clone + '_ {
        self.sm.par_iter().map(|(k, v)| (ClientId(k), v))
    }

    /// Returns a parallel mutable iterator over all clients on the server in an
    /// unspecified order.
    pub fn par_iter_mut(
        &mut self,
    ) -> impl ParallelIterator<Item = (ClientId, &mut Client<C>)> + '_ {
        self.sm.par_iter_mut().map(|(k, v)| (ClientId(k), v))
    }
}

/// An identifier for a [`Client`] on the server.
///
/// Client IDs are either _valid_ or _invalid_. Valid client IDs point to
/// clients that have not been deleted, while invalid IDs point to those that
/// have. Once an ID becomes invalid, it will never become valid again.
///
/// The [`Ord`] instance on this type is correct but otherwise unspecified. This
/// is useful for storing IDs in containers such as
/// [`BTreeMap`](std::collections::BTreeMap).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Debug)]
pub struct ClientId(Key);

impl ClientId {
    /// The value of the default client ID which is always invalid.
    pub const NULL: Self = Self(Key::NULL);
}

/// Represents a remote connection to a client after successfully logging in.
///
/// Much like an [`Entity`], clients posess a location, rotation, and UUID.
/// However, clients are handled separately from entities and are partially
/// managed by the library.
///
/// By default, clients have no influence over the worlds they reside in. They
/// cannot break blocks, hurt entities, or see other clients. Interactions with
/// the server must be handled explicitly with [`Self::pop_event`].
///
/// Additionally, clients posess [`Player`] entity data which is only visible to
/// themselves. This can be accessed with [`Self::player`] and
/// [`Self::player_mut`].
///
/// # The Difference Between a "Client" and a "Player"
///
/// Normally in Minecraft, players and clients are one and the same. Players are
/// simply a special type of entity which is backed by a remote connection.
///
/// In Valence however, clients and players have been decoupled. This separation
/// was done primarily to enable multithreaded client updates.
pub struct Client<C: Config> {
    /// Custom data.
    pub data: C::ClientData,
    /// Setting this to `None` disconnects the client.
    send: SendOpt,
    recv: Receiver<C2sPlayPacket>,
    /// The tick this client was created.
    created_tick: Ticks,
    uuid: Uuid,
    username: String,
    textures: Option<SignedPlayerTextures>,
    world: WorldId,
    new_position: Vec3<f64>,
    old_position: Vec3<f64>,
    /// Measured in m/s.
    velocity: Vec3<f32>,
    /// Measured in degrees
    yaw: f32,
    /// Measured in degrees
    pitch: f32,
    /// Counts up as teleports are made.
    teleport_id_counter: u32,
    /// The number of pending client teleports that have yet to receive a
    /// confirmation. Inbound client position packets are ignored while this
    /// is nonzero.
    pending_teleports: u32,
    spawn_position: BlockPos,
    spawn_position_yaw: f32,
    death_location: Option<(DimensionId, BlockPos)>,
    events: VecDeque<Event>,
    /// The ID of the last keepalive sent.
    last_keepalive_id: i64,
    new_max_view_distance: u8,
    old_max_view_distance: u8,
    /// Entities that were visible to this client at the end of the last tick.
    /// This is used to determine what entity create/destroy packets should be
    /// sent.
    loaded_entities: HashSet<EntityId>,
    loaded_chunks: HashSet<ChunkPos>,
    new_game_mode: GameMode,
    old_game_mode: GameMode,
    settings: Option<Settings>,
    dug_blocks: Vec<i32>,
    /// Should be sent after login packet.
    msgs_to_send: Vec<Text>,
    attack_speed: f64,
    movement_speed: f64,
    flags: ClientFlags,
    /// The data for the client's own player entity.
    player_data: Player,
}

#[bitfield(u16)]
pub(crate) struct ClientFlags {
    spawn: bool,
    sneaking: bool,
    sprinting: bool,
    jumping_with_horse: bool,
    on_ground: bool,
    /// If any of position, yaw, or pitch were modified by the
    /// user this tick.
    teleported_this_tick: bool,
    /// If spawn_position or spawn_position_yaw were modified this tick.
    modified_spawn_position: bool,
    /// If the last sent keepalive got a response.
    got_keepalive: bool,
    hardcore: bool,
    attack_speed_modified: bool,
    movement_speed_modified: bool,
    velocity_modified: bool,
    #[bits(4)]
    _pad: u8,
}

impl<C: Config> Client<C> {
    pub(crate) fn new(
        packet_channels: C2sPacketChannels,
        server: &SharedServer<C>,
        ncd: NewClientData,
        data: C::ClientData,
    ) -> Self {
        let (send, recv) = packet_channels;

        Self {
            data,
            send: Some(send),
            recv,
            created_tick: server.current_tick(),
            uuid: ncd.uuid,
            username: ncd.username,
            textures: ncd.textures,
            world: WorldId::default(),
            new_position: Vec3::default(),
            old_position: Vec3::default(),
            velocity: Vec3::default(),
            yaw: 0.0,
            pitch: 0.0,
            teleport_id_counter: 0,
            pending_teleports: 0,
            spawn_position: BlockPos::default(),
            spawn_position_yaw: 0.0,
            death_location: None,
            events: VecDeque::new(),
            last_keepalive_id: 0,
            new_max_view_distance: 16,
            old_max_view_distance: 0,
            loaded_entities: HashSet::new(),
            loaded_chunks: HashSet::new(),
            new_game_mode: GameMode::Survival,
            old_game_mode: GameMode::Survival,
            settings: None,
            dug_blocks: Vec::new(),
            msgs_to_send: Vec::new(),
            attack_speed: 4.0,
            movement_speed: 0.7,
            flags: ClientFlags::new()
                .with_modified_spawn_position(true)
                .with_got_keepalive(true),
            player_data: Player::new(),
        }
    }

    /// Gets the tick that this client was created.
    pub fn created_tick(&self) -> Ticks {
        self.created_tick
    }

    /// Gets the client's UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Gets the username of this client, which is always valid.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the sneaking state of this client.
    pub fn is_sneaking(&self) -> bool {
        self.flags.sneaking()
    }

    /// Returns the sprinting state of this client.
    pub fn is_sprinting(&self) -> bool {
        self.flags.sprinting()
    }

    /// Gets the player textures of this client. If the client does not have
    /// a skin, then `None` is returned.
    pub fn textures(&self) -> Option<&SignedPlayerTextures> {
        self.textures.as_ref()
    }

    /// Gets the world this client is located in.
    pub fn world(&self) -> WorldId {
        self.world
    }

    /// Changes the world this client is located in.
    ///
    /// The given [`WorldId`] must be valid. Otherwise, the client is
    /// disconnected.
    pub fn spawn(&mut self, world: WorldId) {
        self.world = world;
        self.flags.set_spawn(true);
    }

    /// Sends a system message to the player which is visible in the chat.
    pub fn send_message(&mut self, msg: impl Into<Text>) {
        // We buffer messages because weird things happen if we send them before the
        // login packet.
        self.msgs_to_send.push(msg.into());
    }

    /// Gets the absolute position of this client in the world it is located
    /// in.
    pub fn position(&self) -> Vec3<f64> {
        self.new_position
    }

    /// Changes the position and rotation of this client in the world it is
    /// located in.
    ///
    /// If you want to change the client's world, use [`Self::spawn`].
    pub fn teleport(&mut self, pos: impl Into<Vec3<f64>>, yaw: f32, pitch: f32) {
        self.new_position = pos.into();
        self.yaw = yaw;
        self.pitch = pitch;
        self.velocity = Vec3::default();

        if !self.flags.teleported_this_tick() {
            self.flags.set_teleported_this_tick(true);

            self.pending_teleports = match self.pending_teleports.checked_add(1) {
                Some(n) => n,
                None => {
                    log::warn!("too many pending teleports for {}", self.username());
                    self.disconnect_no_reason();
                    return;
                }
            };

            self.teleport_id_counter = self.teleport_id_counter.wrapping_add(1);
        }
    }

    /// Gets the velocity of this client in m/s.
    ///
    /// The velocity of a client is derived from their current and previous
    /// position.
    pub fn velocity(&self) -> Vec3<f32> {
        self.velocity
    }

    /// Sets the client's velocity in m/s.
    pub fn set_velocity(&mut self, velocity: impl Into<Vec3<f32>>) {
        self.velocity = velocity.into();
        self.flags.set_velocity_modified(true);
    }

    /// Gets this client's yaw.
    pub fn yaw(&self) -> f32 {
        self.yaw
    }

    /// Gets this client's pitch.
    pub fn pitch(&self) -> f32 {
        self.pitch
    }

    /// Gets the spawn position. The client will see `minecraft:compass` items
    /// point at the returned position.
    pub fn spawn_position(&self) -> BlockPos {
        self.spawn_position
    }

    /// Sets the spawn position. The client will see `minecraft:compass` items
    /// point at the provided position.
    pub fn set_spawn_position(&mut self, pos: impl Into<BlockPos>, yaw_degrees: f32) {
        let pos = pos.into();
        if pos != self.spawn_position || yaw_degrees != self.spawn_position_yaw {
            self.spawn_position = pos;
            self.spawn_position_yaw = yaw_degrees;
            self.flags.set_modified_spawn_position(true);
        }
    }

    /// Gets the last death location of this client. The client will see
    /// `minecraft:recovery_compass` items point at the returned position.
    /// If the client's current dimension differs from the returned
    /// dimension or the location is `None` then the compass will spin
    /// randomly.
    pub fn death_location(&self) -> Option<(DimensionId, BlockPos)> {
        self.death_location
    }

    /// Sets the last death location. The client will see
    /// `minecraft:recovery_compass` items point at the provided position.
    /// If the client's current dimension differs from the provided
    /// dimension or the location is `None` then the compass will spin
    /// randomly.
    ///
    /// Changes to the last death location take effect when the client
    /// (re)spawns.
    pub fn set_death_location(&mut self, location: Option<(DimensionId, BlockPos)>) {
        self.death_location = location;
    }

    /// Gets the client's game mode.
    pub fn game_mode(&self) -> GameMode {
        self.new_game_mode
    }

    /// Sets the client's game mode.
    pub fn set_game_mode(&mut self, game_mode: GameMode) {
        self.new_game_mode = game_mode;
    }

    /// Sets the title this client sees.
    ///
    /// A title is a large piece of text displayed in the center of the screen
    /// which may also include a subtitle underneath it. The title
    /// can be configured to fade in and out using the
    /// [`TitleAnimationTimes`] struct.
    pub fn set_title(
        &mut self,
        title: impl Into<Text>,
        subtitle: impl Into<Text>,
        animation: impl Into<Option<TitleAnimationTimes>>,
    ) {
        let title = title.into();
        let subtitle = subtitle.into();

        self.send_packet(SetTitleText { text: title });

        if !subtitle.is_empty() {
            self.send_packet(SetSubtitleText {
                subtitle_text: subtitle,
            });
        }

        if let Some(anim) = animation.into() {
            self.send_packet(anim);
        }
    }

    /// Gets the attack cooldown speed.
    pub fn attack_speed(&self) -> f64 {
        self.attack_speed
    }

    /// Sets the attack cooldown speed.
    pub fn set_attack_speed(&mut self, speed: f64) {
        if self.attack_speed != speed {
            self.attack_speed = speed;
            self.flags.set_attack_speed_modified(true);
        }
    }

    /// Gets the speed at which the client can run on the ground.
    pub fn movement_speed(&self) -> f64 {
        self.movement_speed
    }

    /// Sets the speed at which the client can run on the ground.
    pub fn set_movement_speed(&mut self, speed: f64) {
        if self.movement_speed != speed {
            self.movement_speed = speed;
            self.flags.set_movement_speed_modified(true);
        }
    }

    /// Removes the current title from the client's screen.
    pub fn clear_title(&mut self) {
        self.send_packet(ClearTitles { reset: true });
    }

    /// Gets if the client is on the ground, as determined by the client.
    pub fn on_ground(&self) -> bool {
        self.flags.on_ground()
    }

    /// Gets whether or not the client is connected to the server.
    ///
    /// A disconnected client object will never become reconnected. It is your
    /// responsibility to remove disconnected clients from the [`Clients`]
    /// container.
    pub fn is_disconnected(&self) -> bool {
        self.send.is_none()
    }

    /// Removes an [`Event`] from the event queue.
    ///
    /// If there are no remaining events, `None` is returned.
    ///
    /// Any remaining client events are deleted at the end of the
    /// current tick.
    pub fn pop_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// The current view distance of this client measured in chunks.
    pub fn view_distance(&self) -> u8 {
        self.settings
            .as_ref()
            .map_or(2, |s| s.view_distance)
            .min(self.max_view_distance())
    }

    /// Gets the maximum view distance. The client will not be able to see
    /// chunks and entities past this distance.
    ///
    /// The value returned is measured in chunks.
    pub fn max_view_distance(&self) -> u8 {
        self.new_max_view_distance
    }

    /// Sets the maximum view distance. The client will not be able to see
    /// chunks and entities past this distance.
    ///
    /// The new view distance is measured in chunks and is clamped to `2..=32`.
    pub fn set_max_view_distance(&mut self, dist: u8) {
        self.new_max_view_distance = dist.clamp(2, 32);
    }

    /// Enables hardcore mode. This changes the design of the client's hearts.
    ///
    /// To have any visible effect, this function must be called on the same
    /// tick the client joins the server.
    pub fn set_hardcore(&mut self, hardcore: bool) {
        self.flags.set_hardcore(hardcore);
    }

    /// Gets if hardcore mode is enabled.
    pub fn is_hardcore(&mut self) -> bool {
        self.flags.hardcore()
    }

    /// Gets the client's current settings.
    pub fn settings(&self) -> Option<&Settings> {
        self.settings.as_ref()
    }

    /// Disconnects this client from the server with the provided reason. This
    /// has no effect if the client is already disconnected.
    ///
    /// All future calls to [`Self::is_disconnected`] will return `true`.
    pub fn disconnect(&mut self, reason: impl Into<Text>) {
        if self.send.is_some() {
            let txt = reason.into();
            log::info!("disconnecting client '{}': \"{txt}\"", self.username);

            self.send_packet(Disconnect { reason: txt });

            self.send = None;
        }
    }

    /// Like [`Self::disconnect`], but no reason for the disconnect is
    /// displayed.
    pub fn disconnect_no_reason(&mut self) {
        if self.send.is_some() {
            log::info!("disconnecting client '{}'", self.username);
            self.send = None;
        }
    }

    /// Returns an immutable reference to the client's own [`Player`] data.
    pub fn player(&self) -> &Player {
        &self.player_data
    }

    /// Returns a mutable reference to the client's own [`Player`] data.
    ///
    /// Changes made to this data is only visible to this client.
    pub fn player_mut(&mut self) -> &mut Player {
        &mut self.player_data
    }

    /// Attempts to enqueue a play packet to be sent to this client. The client
    /// is disconnected if the clientbound packet buffer is full.
    #[cfg(feature = "protocol")]
    pub fn send_packet(&mut self, packet: impl Into<S2cPlayPacket>) {
        send_packet(&mut self.send, packet);
    }

    #[cfg(not(feature = "protocol"))]
    pub(crate) fn send_packet(&mut self, packet: impl Into<S2cPlayPacket>) {
        send_packet(&mut self.send, packet);
    }

    pub(crate) fn handle_serverbound_packets(&mut self, entities: &Entities<C>) {
        self.events.clear();
        for _ in 0..self.recv.len() {
            self.handle_serverbound_packet(entities, self.recv.try_recv().unwrap());
        }
    }

    fn handle_serverbound_packet(&mut self, entities: &Entities<C>, pkt: C2sPlayPacket) {
        fn handle_movement_packet<C: Config>(
            client: &mut Client<C>,
            _vehicle: bool,
            new_position: Vec3<f64>,
            new_yaw: f32,
            new_pitch: f32,
            new_on_ground: bool,
        ) {
            if client.pending_teleports == 0 {
                // TODO: validate movement using swept AABB collision with the blocks.
                // TODO: validate that the client is actually inside/outside the vehicle?

                // Movement packets should be coming in at a rate of STANDARD_TPS.
                let new_velocity = (new_position - client.new_position).as_() * STANDARD_TPS as f32;

                let event = Event::Movement {
                    old_position: client.new_position,
                    old_velocity: client.velocity,
                    old_yaw: client.yaw,
                    old_pitch: client.pitch,
                    old_on_ground: client.flags.on_ground(),
                    new_position,
                    new_velocity,
                    new_yaw,
                    new_pitch,
                    new_on_ground,
                };

                client.new_position = new_position;
                client.velocity = new_velocity;
                client.yaw = new_yaw;
                client.pitch = new_pitch;
                client.flags.set_on_ground(new_on_ground);

                client.events.push_back(event);
            }
        }

        match pkt {
            C2sPlayPacket::AcceptTeleportation(p) => {
                if self.pending_teleports == 0 {
                    log::warn!("unexpected teleport confirmation from {}", self.username());
                    self.disconnect_no_reason();
                    return;
                }

                let got = p.teleport_id.0 as u32;
                let expected = self
                    .teleport_id_counter
                    .wrapping_sub(self.pending_teleports);

                if got == expected {
                    self.pending_teleports -= 1;
                } else {
                    log::warn!(
                        "unexpected teleport ID from {} (expected {expected}, got {got})",
                        self.username()
                    );
                    self.disconnect_no_reason();
                }
            }
            C2sPlayPacket::BlockEntityTagQuery(_) => {}
            C2sPlayPacket::ChangeDifficulty(_) => {}
            C2sPlayPacket::ChatCommand(_) => {}
            C2sPlayPacket::Chat(p) => self.events.push_back(Event::ChatMessage {
                message: p.message.0,
                timestamp: Duration::from_millis(p.timestamp),
            }),
            C2sPlayPacket::ChatPreview(_) => {}
            C2sPlayPacket::ClientCommand(_) => {}
            C2sPlayPacket::ClientInformation(p) => {
                let old = self.settings.replace(Settings {
                    locale: p.locale.0,
                    view_distance: p.view_distance.0,
                    chat_mode: p.chat_mode,
                    chat_colors: p.chat_colors,
                    main_hand: p.main_hand,
                    displayed_skin_parts: p.displayed_skin_parts,
                    allow_server_listings: p.allow_server_listings,
                });

                self.events.push_back(Event::SettingsChanged(old));
            }
            C2sPlayPacket::CommandSuggestion(_) => {}
            C2sPlayPacket::ContainerButtonClick(_) => {}
            C2sPlayPacket::ContainerClose(_) => {}
            C2sPlayPacket::CustomPayload(_) => {}
            C2sPlayPacket::EditBook(_) => {}
            C2sPlayPacket::EntityTagQuery(_) => {}
            C2sPlayPacket::Interact(p) => {
                if let Some(id) = entities.get_with_network_id(p.entity_id.0) {
                    // TODO: verify that the client has line of sight to the targeted entity and
                    // that the distance is <=4 blocks.

                    self.events.push_back(Event::InteractWithEntity {
                        id,
                        sneaking: p.sneaking,
                        kind: match p.kind {
                            InteractKind::Interact(hand) => InteractWithEntityKind::Interact(hand),
                            InteractKind::Attack => InteractWithEntityKind::Attack,
                            InteractKind::InteractAt((target, hand)) => {
                                InteractWithEntityKind::InteractAt { target, hand }
                            }
                        },
                    });
                }
            }
            C2sPlayPacket::JigsawGenerate(_) => {}
            C2sPlayPacket::KeepAlive(p) => {
                let last_keepalive_id = self.last_keepalive_id;
                if self.flags.got_keepalive() {
                    log::warn!("unexpected keepalive from player {}", self.username());
                    self.disconnect_no_reason();
                } else if p.id != last_keepalive_id {
                    log::warn!(
                        "keepalive ids for player {} don't match (expected {}, got {})",
                        self.username(),
                        last_keepalive_id,
                        p.id
                    );
                    self.disconnect_no_reason();
                } else {
                    self.flags.set_got_keepalive(true);
                }
            }
            C2sPlayPacket::LockDifficulty(_) => {}
            C2sPlayPacket::MovePlayerPosition(p) => {
                handle_movement_packet(self, false, p.position, self.yaw, self.pitch, p.on_ground)
            }
            C2sPlayPacket::MovePlayerPositionAndRotation(p) => {
                handle_movement_packet(self, false, p.position, p.yaw, p.pitch, p.on_ground)
            }
            C2sPlayPacket::MovePlayerRotation(p) => {
                handle_movement_packet(self, false, self.new_position, p.yaw, p.pitch, p.on_ground)
            }
            C2sPlayPacket::MovePlayerStatusOnly(p) => handle_movement_packet(
                self,
                false,
                self.new_position,
                self.yaw,
                self.pitch,
                p.on_ground,
            ),
            C2sPlayPacket::MoveVehicle(p) => {
                handle_movement_packet(
                    self,
                    true,
                    p.position,
                    p.yaw,
                    p.pitch,
                    self.flags.on_ground(),
                );
            }
            C2sPlayPacket::PaddleBoat(p) => {
                self.events.push_back(Event::SteerBoat {
                    left_paddle_turning: p.left_paddle_turning,
                    right_paddle_turning: p.right_paddle_turning,
                });
            }
            C2sPlayPacket::PickItem(_) => {}
            C2sPlayPacket::PlaceRecipe(_) => {}
            C2sPlayPacket::PlayerAbilities(_) => {}
            C2sPlayPacket::PlayerAction(p) => {
                // TODO: verify dug block is within the correct distance from the client.
                // TODO: verify that the broken block is allowed to be broken?

                if p.sequence.0 != 0 {
                    self.dug_blocks.push(p.sequence.0);
                }

                self.events.push_back(match p.status {
                    DiggingStatus::StartedDigging => Event::Digging {
                        status: event::DiggingStatus::Start,
                        position: p.location,
                        face: p.face,
                    },
                    DiggingStatus::CancelledDigging => Event::Digging {
                        status: event::DiggingStatus::Cancel,
                        position: p.location,
                        face: p.face,
                    },
                    DiggingStatus::FinishedDigging => Event::Digging {
                        status: event::DiggingStatus::Finish,
                        position: p.location,
                        face: p.face,
                    },
                    DiggingStatus::DropItemStack => return,
                    DiggingStatus::DropItem => return,
                    DiggingStatus::ShootArrowOrFinishEating => return,
                    DiggingStatus::SwapItemInHand => return,
                });
            }
            C2sPlayPacket::PlayerCommand(e) => {
                // TODO: validate:
                // - Can't sprint and sneak at the same time
                // - Can't leave bed while not in a bed.
                // - Can't jump with a horse if not on a horse
                // - Can't open horse inventory if not on a horse.
                // - Can't fly with elytra if not wearing an elytra.
                // - Can't jump with horse while already jumping & vice versa?
                self.events.push_back(match e.action_id {
                    PlayerCommandId::StartSneaking => {
                        if self.flags.sneaking() {
                            return;
                        }
                        self.flags.set_sneaking(true);
                        Event::StartSneaking
                    }
                    PlayerCommandId::StopSneaking => {
                        if !self.flags.sneaking() {
                            return;
                        }
                        self.flags.set_sneaking(false);
                        Event::StopSneaking
                    }
                    PlayerCommandId::LeaveBed => Event::LeaveBed,
                    PlayerCommandId::StartSprinting => {
                        if self.flags.sprinting() {
                            return;
                        }
                        self.flags.set_sprinting(true);
                        Event::StartSprinting
                    }
                    PlayerCommandId::StopSprinting => {
                        if !self.flags.sprinting() {
                            return;
                        }
                        self.flags.set_sprinting(false);
                        Event::StopSprinting
                    }
                    PlayerCommandId::StartJumpWithHorse => {
                        self.flags.set_jumping_with_horse(true);
                        Event::StartJumpWithHorse {
                            jump_boost: e.jump_boost.0 .0 as u8,
                        }
                    }
                    PlayerCommandId::StopJumpWithHorse => {
                        self.flags.set_jumping_with_horse(false);
                        Event::StopJumpWithHorse
                    }
                    PlayerCommandId::OpenHorseInventory => Event::OpenHorseInventory,
                    PlayerCommandId::StartFlyingWithElytra => Event::StartFlyingWithElytra,
                });
            }
            C2sPlayPacket::PlayerInput(_) => {}
            C2sPlayPacket::Pong(_) => {}
            C2sPlayPacket::RecipeBookChangeSettings(_) => {}
            C2sPlayPacket::RecipeBookSeenRecipe(_) => {}
            C2sPlayPacket::RenameItem(_) => {}
            C2sPlayPacket::ResourcePack(_) => {}
            C2sPlayPacket::SeenAdvancements(_) => {}
            C2sPlayPacket::SelectTrade(_) => {}
            C2sPlayPacket::SetBeacon(_) => {}
            C2sPlayPacket::SetCarriedItem(_) => {}
            C2sPlayPacket::SetCommandBlock(_) => {}
            C2sPlayPacket::SetCommandBlockMinecart(_) => {}
            C2sPlayPacket::SetCreativeModeSlot(_) => {}
            C2sPlayPacket::SetJigsawBlock(_) => {}
            C2sPlayPacket::SetStructureBlock(_) => {}
            C2sPlayPacket::SignUpdate(_) => {}
            C2sPlayPacket::Swing(p) => self.events.push_back(Event::ArmSwing(p.hand)),
            C2sPlayPacket::TeleportToEntity(_) => {}
            C2sPlayPacket::UseItemOn(_) => {}
            C2sPlayPacket::UseItem(_) => {}
        }
    }

    pub(crate) fn update(
        &mut self,
        shared: &SharedServer<C>,
        entities: &Entities<C>,
        worlds: &Worlds<C>,
    ) {
        // Mark the client as disconnected when appropriate.
        if self.recv.is_disconnected() || self.send.as_ref().map_or(true, |s| s.is_disconnected()) {
            self.send = None;
            return;
        }

        let world = match worlds.get(self.world) {
            Some(world) => world,
            None => {
                log::warn!(
                    "client {} is in an invalid world and must be disconnected",
                    self.username()
                );
                self.disconnect_no_reason();
                return;
            }
        };

        let current_tick = shared.current_tick();

        // Send the join game packet and other initial packets. We defer this until now
        // so that the user can set the client's location, game mode, etc.
        if self.created_tick == current_tick {
            world
                .meta
                .player_list()
                .initial_packets(|pkt| self.send_packet(pkt));

            let mut dimension_names: Vec<_> = shared
                .dimensions()
                .map(|(id, _)| ident!("{LIBRARY_NAMESPACE}:dimension_{}", id.0))
                .collect();

            dimension_names.push(ident!("{LIBRARY_NAMESPACE}:dummy_dimension"));

            self.send_packet(Login {
                entity_id: 0, // EntityId 0 is reserved for clients.
                is_hardcore: self.flags.hardcore(),
                gamemode: self.new_game_mode,
                previous_gamemode: self.old_game_mode,
                dimension_names,
                registry_codec: Nbt(make_registry_codec(shared)),
                dimension_type_name: ident!(
                    "{LIBRARY_NAMESPACE}:dimension_type_{}",
                    world.meta.dimension().0
                ),
                dimension_name: ident!(
                    "{LIBRARY_NAMESPACE}:dimension_{}",
                    world.meta.dimension().0
                ),
                hashed_seed: 0,
                max_players: VarInt(0),
                view_distance: BoundedInt(VarInt(self.new_max_view_distance as i32)),
                simulation_distance: VarInt(16),
                reduced_debug_info: false,
                enable_respawn_screen: false,
                is_debug: false,
                is_flat: world.meta.is_flat(),
                last_death_location: self
                    .death_location
                    .map(|(id, pos)| (ident!("{LIBRARY_NAMESPACE}:dimension_{}", id.0), pos)),
            });

            self.teleport(self.position(), self.yaw(), self.pitch());
        } else {
            if self.flags.spawn() {
                self.flags.set_spawn(false);
                self.loaded_entities.clear();
                self.loaded_chunks.clear();

                // TODO: clear player list.

                // Client bug workaround: send the client to a dummy dimension first.
                self.send_packet(Respawn {
                    dimension_type_name: ident!("{LIBRARY_NAMESPACE}:dimension_type_0"),
                    dimension_name: ident!("{LIBRARY_NAMESPACE}:dummy_dimension"),
                    hashed_seed: 0,
                    game_mode: self.game_mode(),
                    previous_game_mode: self.game_mode(),
                    is_debug: false,
                    is_flat: false,
                    copy_metadata: true,
                    last_death_location: None,
                });

                self.send_packet(Respawn {
                    dimension_type_name: ident!(
                        "{LIBRARY_NAMESPACE}:dimension_type_{}",
                        world.meta.dimension().0
                    ),
                    dimension_name: ident!(
                        "{LIBRARY_NAMESPACE}:dimension_{}",
                        world.meta.dimension().0
                    ),
                    hashed_seed: 0,
                    game_mode: self.game_mode(),
                    previous_game_mode: self.game_mode(),
                    is_debug: false,
                    is_flat: world.meta.is_flat(),
                    copy_metadata: true,
                    last_death_location: self
                        .death_location
                        .map(|(id, pos)| (ident!("{LIBRARY_NAMESPACE}:dimension_{}", id.0), pos)),
                });

                self.teleport(self.position(), self.yaw(), self.pitch());
            }

            if self.old_game_mode != self.new_game_mode {
                self.old_game_mode = self.new_game_mode;
                self.send_packet(GameEvent {
                    reason: GameEventReason::ChangeGameMode,
                    value: self.new_game_mode as i32 as f32,
                });
            }

            world
                .meta
                .player_list()
                .diff_packets(|pkt| self.send_packet(pkt));
        }

        // Set player attributes
        if self.flags.attack_speed_modified() {
            self.flags.set_attack_speed_modified(false);

            self.send_packet(UpdateAttributes {
                entity_id: VarInt(0),
                properties: vec![UpdateAttributesProperty {
                    key: ident!("generic.attack_speed"),
                    value: self.attack_speed,
                    modifiers: Vec::new(),
                }],
            });
        }

        if self.flags.movement_speed_modified() {
            self.flags.set_movement_speed_modified(false);

            self.send_packet(UpdateAttributes {
                entity_id: VarInt(0),
                properties: vec![UpdateAttributesProperty {
                    key: ident!("generic.movement_speed"),
                    value: self.movement_speed,
                    modifiers: Vec::new(),
                }],
            });
        }

        // Update the players spawn position (compass position)
        if self.flags.modified_spawn_position() {
            self.flags.set_modified_spawn_position(false);

            self.send_packet(SpawnPosition {
                location: self.spawn_position,
                angle: self.spawn_position_yaw,
            })
        }

        // Update view distance fog on the client if necessary.
        if self.old_max_view_distance != self.new_max_view_distance {
            self.old_max_view_distance = self.new_max_view_distance;
            if self.created_tick != current_tick {
                self.send_packet(SetChunkCacheRadius {
                    view_distance: BoundedInt(VarInt(self.new_max_view_distance as i32)),
                })
            }
        }

        // Check if it's time to send another keepalive.
        if current_tick % (shared.tick_rate() * 8) == 0 {
            if self.flags.got_keepalive() {
                let id = rand::random();
                self.send_packet(KeepAlive { id });
                self.last_keepalive_id = id;
                self.flags.set_got_keepalive(false);
            } else {
                log::warn!(
                    "player {} timed out (no keepalive response)",
                    self.username()
                );
                self.disconnect_no_reason();
            }
        }

        let view_dist = self.view_distance();

        let center = ChunkPos::at(self.new_position.x, self.new_position.z);

        // Send the update view position packet if the client changes the chunk section
        // they're in.
        {
            let old_section = self.old_position.map(|n| (n / 16.0).floor() as i32);
            let new_section = self.new_position.map(|n| (n / 16.0).floor() as i32);

            if old_section != new_section {
                self.send_packet(SetChunkCacheCenter {
                    chunk_x: VarInt(new_section.x),
                    chunk_z: VarInt(new_section.z),
                })
            }
        }

        let dimension = shared.dimension(world.meta.dimension());

        // Update existing chunks and unload those outside the view distance. Chunks
        // that have been overwritten also need to be unloaded.
        self.loaded_chunks.retain(|&pos| {
            // The cache stops chunk data packets from needing to be sent when a player
            // moves to an adjacent chunk and back to the original.
            let cache = 2;

            if let Some(chunk) = world.chunks.get(pos) {
                if is_chunk_in_view_distance(center, pos, view_dist + cache)
                    && chunk.created_tick() != current_tick
                {
                    chunk.block_change_packets(pos, dimension.min_y, |pkt| {
                        send_packet(&mut self.send, pkt)
                    });
                    return true;
                }
            }

            send_packet(
                &mut self.send,
                ForgetLevelChunk {
                    chunk_x: pos.x,
                    chunk_z: pos.z,
                },
            );
            false
        });

        // Load new chunks within the view distance
        for pos in chunks_in_view_distance(center, view_dist) {
            if let Some(chunk) = world.chunks.get(pos) {
                if self.loaded_chunks.insert(pos) {
                    self.send_packet(chunk.chunk_data_packet(pos));
                    chunk.block_change_packets(pos, dimension.min_y, |pkt| self.send_packet(pkt));
                }
            }
        }

        // Acknowledge broken blocks.
        for seq in self.dug_blocks.drain(..) {
            send_packet(
                &mut self.send,
                BlockChangeAck {
                    sequence: VarInt(seq),
                },
            )
        }

        // Teleport the player.
        //
        // This is done after the chunks are loaded so that the "downloading terrain"
        // screen is closed at the appropriate time.
        if self.flags.teleported_this_tick() {
            self.flags.set_teleported_this_tick(false);

            self.send_packet(PlayerPosition {
                position: self.new_position,
                yaw: self.yaw,
                pitch: self.pitch,
                flags: PlayerPositionFlags::new(false, false, false, false, false),
                teleport_id: VarInt((self.teleport_id_counter - 1) as i32),
                dismount_vehicle: false,
            });
        }

        // Set velocity. Do this after teleporting since teleporting sets velocity to
        // zero.
        if self.flags.velocity_modified() {
            self.flags.set_velocity_modified(false);

            self.send_packet(SetEntityMotion {
                entity_id: VarInt(0),
                velocity: velocity_to_packet_units(self.velocity),
            });
        }

        // Send chat messages.
        for msg in self.msgs_to_send.drain(..) {
            send_packet(
                &mut self.send,
                SystemChat {
                    chat: msg,
                    kind: VarInt(0),
                },
            );
        }

        let mut entities_to_unload = Vec::new();

        // Update all entities that are visible and unload entities that are no
        // longer visible.
        self.loaded_entities.retain(|&id| {
            if let Some(entity) = entities.get(id) {
                debug_assert!(entity.kind() != EntityKind::Marker);
                if self.new_position.distance(entity.position()) <= view_dist as f64 * 16.0 {
                    if let Some(meta) = entity.updated_metadata_packet(id) {
                        send_packet(&mut self.send, meta);
                    }

                    let position_delta = entity.position() - entity.old_position();
                    let needs_teleport = position_delta.map(f64::abs).reduce_partial_max() >= 8.0;
                    let flags = entity.flags();

                    if entity.position() != entity.old_position()
                        && !needs_teleport
                        && flags.yaw_or_pitch_modified()
                    {
                        send_packet(
                            &mut self.send,
                            MoveEntityPositionAndRotation {
                                entity_id: VarInt(id.to_network_id()),
                                delta: (position_delta * 4096.0).as_(),
                                yaw: ByteAngle::from_degrees(entity.yaw()),
                                pitch: ByteAngle::from_degrees(entity.pitch()),
                                on_ground: entity.on_ground(),
                            },
                        );
                    } else {
                        if entity.position() != entity.old_position() && !needs_teleport {
                            send_packet(
                                &mut self.send,
                                MoveEntityPosition {
                                    entity_id: VarInt(id.to_network_id()),
                                    delta: (position_delta * 4096.0).as_(),
                                    on_ground: entity.on_ground(),
                                },
                            );
                        }

                        if flags.yaw_or_pitch_modified() {
                            send_packet(
                                &mut self.send,
                                MoveEntityRotation {
                                    entity_id: VarInt(id.to_network_id()),
                                    yaw: ByteAngle::from_degrees(entity.yaw()),
                                    pitch: ByteAngle::from_degrees(entity.pitch()),
                                    on_ground: entity.on_ground(),
                                },
                            );
                        }
                    }

                    if needs_teleport {
                        send_packet(
                            &mut self.send,
                            TeleportEntity {
                                entity_id: VarInt(id.to_network_id()),
                                position: entity.position(),
                                yaw: ByteAngle::from_degrees(entity.yaw()),
                                pitch: ByteAngle::from_degrees(entity.pitch()),
                                on_ground: entity.on_ground(),
                            },
                        );
                    }

                    if flags.velocity_modified() {
                        send_packet(
                            &mut self.send,
                            SetEntityMotion {
                                entity_id: VarInt(id.to_network_id()),
                                velocity: velocity_to_packet_units(entity.velocity()),
                            },
                        );
                    }

                    if flags.head_yaw_modified() {
                        send_packet(
                            &mut self.send,
                            RotateHead {
                                entity_id: VarInt(id.to_network_id()),
                                head_yaw: ByteAngle::from_degrees(entity.head_yaw()),
                            },
                        )
                    }

                    send_entity_events(&mut self.send, id, entity);

                    return true;
                }
            }

            entities_to_unload.push(VarInt(id.to_network_id()));
            false
        });

        if !entities_to_unload.is_empty() {
            self.send_packet(RemoveEntities {
                entities: entities_to_unload,
            });
        }

        // Update the client's own player metadata.
        let mut data = Vec::new();
        self.player_data.updated_metadata(&mut data);

        if !data.is_empty() {
            data.push(0xff);

            self.send_packet(SetEntityMetadata {
                entity_id: VarInt(0),
                metadata: RawBytes(data),
            });
        }

        // Spawn new entities within the view distance.
        let pos = self.position();
        world.spatial_index.query::<_, _, ()>(
            |bb| bb.projected_point(pos).distance(pos) <= view_dist as f64 * 16.0,
            |id, _| {
                let entity = entities
                    .get(id)
                    .expect("entity IDs in spatial index should be valid at this point");
                if entity.kind() != EntityKind::Marker
                    && entity.uuid() != self.uuid
                    && self.loaded_entities.insert(id)
                {
                    self.send_packet(
                        entity
                            .spawn_packet(id)
                            .expect("should not be a marker entity"),
                    );

                    if let Some(meta) = entity.initial_metadata_packet(id) {
                        self.send_packet(meta);
                    }

                    send_entity_events(&mut self.send, id, entity);
                }
                None
            },
        );

        for &code in self.player_data.event_codes() {
            if code <= ENTITY_EVENT_MAX_BOUND as u8 {
                send_packet(
                    &mut self.send,
                    EntityEvent {
                        entity_id: 0,
                        entity_status: BoundedInt(code),
                    },
                );
            }
            // Don't bother sending animations for self since it shouldn't have
            // any effect.
        }

        self.player_data.clear_modifications();
        self.old_position = self.new_position;
    }
}

type SendOpt = Option<Sender<S2cPlayPacket>>;

fn send_packet(send_opt: &mut SendOpt, pkt: impl Into<S2cPlayPacket>) {
    if let Some(send) = send_opt {
        match send.try_send(pkt.into()) {
            Err(TrySendError::Full(_)) => {
                log::warn!("max outbound packet capacity reached for client");
                *send_opt = None;
            }
            Err(TrySendError::Disconnected(_)) => {
                *send_opt = None;
            }
            Ok(_) => {}
        }
    }
}

fn send_entity_events<C: Config>(send_opt: &mut SendOpt, id: EntityId, entity: &Entity<C>) {
    for &code in entity.state.event_codes() {
        if code <= ENTITY_EVENT_MAX_BOUND as u8 {
            send_packet(
                send_opt,
                EntityEvent {
                    entity_id: id.to_network_id(),
                    entity_status: BoundedInt(code),
                },
            );
        } else {
            send_packet(
                send_opt,
                Animate {
                    entity_id: VarInt(id.to_network_id()),
                    animation: BoundedInt(code - ENTITY_EVENT_MAX_BOUND as u8 - 1),
                },
            )
        }
    }
}

fn make_registry_codec<C: Config>(shared: &SharedServer<C>) -> RegistryCodec {
    let mut dims = Vec::new();
    for (id, dim) in shared.dimensions() {
        let id = id.0 as i32;
        dims.push(DimensionTypeRegistryEntry {
            name: ident!("{LIBRARY_NAMESPACE}:dimension_type_{id}"),
            id,
            element: dim.to_dimension_registry_item(),
        })
    }

    let mut biomes: Vec<_> = shared
        .biomes()
        .map(|(id, biome)| biome.to_biome_registry_item(id.0 as i32))
        .collect();

    // The client needs a biome named "minecraft:plains" in the registry to
    // connect. This is probably a bug.
    //
    // If the issue is resolved, just delete this block.
    if !biomes.iter().any(|b| b.name == ident!("plains")) {
        let biome = Biome::default();
        assert_eq!(biome.name, ident!("plains"));
        biomes.push(biome.to_biome_registry_item(biomes.len() as i32));
    }

    RegistryCodec {
        dimension_type_registry: DimensionTypeRegistry {
            kind: ident!("dimension_type"),
            value: dims,
        },
        biome_registry: BiomeRegistry {
            kind: ident!("worldgen/biome"),
            value: biomes,
        },
        chat_type_registry: ChatTypeRegistry {
            kind: ident!("chat_type"),
            value: vec![ChatTypeRegistryEntry {
                name: ident!("system"),
                id: 0,
                element: ChatType {
                    chat: ChatTypeChat {},
                    narration: ChatTypeNarration {
                        priority: "system".into(),
                    },
                },
            }],
        },
    }
}
