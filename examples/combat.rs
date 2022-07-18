use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};

use log::LevelFilter;
use valence::block::{BlockPos, BlockState};
use valence::client::Event::{self};
use valence::client::{ClientId, GameMode, Hand, InteractWithEntityKind};
use valence::config::{Config, ServerListPing};
use valence::dimension::DimensionId;
use valence::entity::state::Pose;
use valence::entity::{EntityId, EntityKind, EntityState};
use valence::server::{Server, SharedServer, ShutdownResult};
use valence::text::{Color, TextFormat};
use valence::{async_trait, Ticks};
use vek::Vec3;

pub fn main() -> ShutdownResult {
    env_logger::Builder::new()
        .filter_module("valence", LevelFilter::Trace)
        .parse_default_env()
        .init();

    valence::start_server(
        Game {
            player_count: AtomicUsize::new(0),
        },
        (),
    )
}

struct Game {
    player_count: AtomicUsize,
}

#[derive(Default)]
struct ClientData {
    /// The client's player entity.
    player: EntityId,
    /// The extra knockback on the first hit while sprinting.
    has_extra_knockback: bool,
}

#[derive(Default)]
struct EntityData {
    client: ClientId,
    attacked: bool,
    attacker_pos: Vec3<f64>,
    extra_knockback: bool,
    last_attack_time: Ticks,
}

const MAX_PLAYERS: usize = 10;

const SPAWN_POS: BlockPos = BlockPos::new(0, 20, 0);

#[async_trait]
impl Config for Game {
    type ChunkData = ();
    type ClientData = ClientData;
    type EntityData = EntityData;
    type ServerData = ();
    type WorldData = ();

    fn max_connections(&self) -> usize {
        // We want status pings to be successful even if the server is full.
        MAX_PLAYERS + 64
    }

    fn online_mode(&self) -> bool {
        // You'll want this to be true on real servers.
        false
    }

    async fn server_list_ping(
        &self,
        _server: &SharedServer<Self>,
        _remote_addr: SocketAddr,
    ) -> ServerListPing {
        ServerListPing::Respond {
            online_players: self.player_count.load(Ordering::SeqCst) as i32,
            max_players: MAX_PLAYERS as i32,
            description: "Hello Valence!".color(Color::AQUA),
            favicon_png: Some(include_bytes!("../assets/favicon.png")),
        }
    }

    fn init(&self, server: &mut Server<Self>) {
        let (_, world) = server.worlds.create(DimensionId::default(), ());
        world.meta.set_flat(true);

        let min_y = server.shared.dimension(DimensionId::default()).min_y;

        // Create circular arena.
        let size = 2;
        for chunk_z in -size - 2..size + 2 {
            for chunk_x in -size - 2..size + 2 {
                let chunk = world.chunks.create([chunk_x, chunk_z], ());
                let r = -size..size;
                if r.contains(&chunk_x) && r.contains(&chunk_z) {
                    for z in 0..16 {
                        for x in 0..16 {
                            let block_x = chunk_x * 16 + x as i32;
                            let block_z = chunk_z * 16 + z as i32;
                            if f64::hypot(block_x as f64, block_z as f64) <= size as f64 * 16.0 {
                                for y in 0..(SPAWN_POS.y - min_y + 1) as usize {
                                    chunk.set_block_state(x, y, z, BlockState::STONE);
                                }
                            }
                        }
                    }
                }
            }
        }

        world.chunks.set_block_state(SPAWN_POS, BlockState::BEDROCK);
    }

    fn update(&self, server: &mut Server<Self>) {
        let (world_id, world) = server.worlds.iter_mut().next().unwrap();

        let current_tick = server.shared.current_tick();

        server.clients.retain(|client_id, client| {
            if client.created_tick() == current_tick {
                if self
                    .player_count
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                        (count < MAX_PLAYERS).then_some(count + 1)
                    })
                    .is_err()
                {
                    client.disconnect("The server is full!".color(Color::RED));
                    return false;
                }

                client.spawn(world_id);
                client.set_game_mode(GameMode::Survival);
                client.teleport(
                    [
                        SPAWN_POS.x as f64 + 0.5,
                        SPAWN_POS.y as f64 + 1.0,
                        SPAWN_POS.z as f64 + 0.5,
                    ],
                    0.0,
                    0.0,
                );

                world.meta.player_list_mut().insert(
                    client.uuid(),
                    client.username().to_owned(),
                    client.textures().cloned(),
                    client.game_mode(),
                    0,
                    None,
                );

                let (player_id, player) = server
                    .entities
                    .create_with_uuid(EntityKind::Player, client.uuid(), EntityData::default())
                    .unwrap();

                client.data.player = player_id;
                client.data.has_extra_knockback = true;

                player.data.client = client_id;
                player.data.last_attack_time = 0;

                client.send_message("Welcome to the arena.".italic());
                if self.player_count.load(Ordering::SeqCst) <= 1 {
                    client.send_message("Have another player join the game with you.".italic());
                }
            }

            if client.is_disconnected() {
                self.player_count.fetch_sub(1, Ordering::SeqCst);
                server.entities.delete(client.data.player);
                world.meta.player_list_mut().remove(client.uuid());
                return false;
            }

            while let Some(event) = client.pop_event() {
                match event {
                    Event::StartSprinting => {
                        client.data.has_extra_knockback = true;
                    }
                    Event::InteractWithEntity {
                        id,
                        kind: InteractWithEntityKind::Attack,
                        ..
                    } => {
                        if let Some(target) = server.entities.get_mut(id) {
                            if !target.data.attacked
                                && current_tick - target.data.last_attack_time >= 10
                                && id != client.data.player
                            {
                                target.data.attacked = true;
                                target.data.attacker_pos = client.position();
                                target.data.extra_knockback = client.data.has_extra_knockback;
                                target.data.last_attack_time = current_tick;

                                client.data.has_extra_knockback = false;
                            }
                        }
                    }
                    Event::ArmSwing(hand) => {
                        let player = server.entities.get_mut(client.data.player).unwrap();

                        if let EntityState::Player(e) = &mut player.state {
                            match hand {
                                Hand::Main => e.trigger_swing_main_arm(),
                                Hand::Off => e.trigger_swing_offhand(),
                            }
                        }
                    }
                    _ => (),
                }
            }

            if client.position().y <= 0.0 {
                client.teleport(
                    [
                        SPAWN_POS.x as f64 + 0.5,
                        SPAWN_POS.y as f64 + 1.0,
                        SPAWN_POS.z as f64 + 0.5,
                    ],
                    client.yaw(),
                    client.pitch(),
                );
            }

            let player = server.entities.get_mut(client.data.player).unwrap();

            player.set_world(client.world());
            player.set_position(client.position());
            player.set_yaw(client.yaw());
            player.set_head_yaw(client.yaw());
            player.set_pitch(client.pitch());
            player.set_on_ground(client.on_ground());

            if let EntityState::Player(player) = &mut player.state {
                if client.is_sneaking() {
                    player.set_pose(Pose::Sneaking);
                } else {
                    player.set_pose(Pose::Standing);
                }

                player.set_sprinting(client.is_sprinting());
            }

            true
        });

        for (_, e) in server.entities.iter_mut() {
            if e.data.attacked {
                e.data.attacked = false;
                let victim = server.clients.get_mut(e.data.client).unwrap();

                let mut vel = (victim.position() - e.data.attacker_pos).normalized();

                let knockback_xz = if e.data.extra_knockback { 18.0 } else { 8.0 };
                let knockback_y = if e.data.extra_knockback { 8.432 } else { 6.432 };

                vel.x *= knockback_xz;
                vel.y = knockback_y;
                vel.z *= knockback_xz;

                victim.set_velocity(victim.velocity() / 2.0 + vel.as_());

                if let EntityState::Player(e) = &mut e.state {
                    e.trigger_take_damage();
                    e.trigger_hurt();
                }
                victim.player_mut().trigger_take_damage();
                victim.player_mut().trigger_hurt();
            }
        }
    }
}
