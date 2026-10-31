//! End-to-end tests for Iroh session admission and datagram IO.

use {
    aeronet_io::{
        Session,
        connection::{Disconnect, DisconnectReason, Disconnected},
    },
    aeronet_iroh::{
        IrohPlugin, IrohRuntime,
        endpoint::IrohEndpoint,
        session::{IrohIo, IrohSession, SessionRequest, SessionResponse, SessionSide},
    },
    bevy::{ecs::system::EntityCommand, prelude::*},
    bytes::Bytes,
    core::time::Duration,
    futures::channel::oneshot,
    iroh::endpoint::presets,
    std::{thread, time::Instant},
};

const TIMEOUT: Duration = Duration::from_secs(10);
const ALPN: &[u8] = b"aeronet-iroh/tests/0";

#[derive(Debug, Clone, PartialEq, Eq)]
enum RecordedDisconnect {
    User(String),
    Peer(String),
    Error(String),
}

#[derive(Debug, Default, Resource)]
struct Disconnects(Vec<(Entity, RecordedDisconnect)>);

#[derive(Debug, Default, Resource)]
struct Requests(Vec<(Entity, usize)>);

#[test]
fn requests_identify_the_exact_connection() {
    let mut app = test_app(SessionResponse::Accepted);
    let endpoint_a = open_endpoint(&mut app);
    let endpoint_b = open_endpoint(&mut app);
    wait_until(&mut app, |world| {
        world.get::<IrohEndpoint>(endpoint_a).is_some()
            && world.get::<IrohEndpoint>(endpoint_b).is_some()
    });
    for count in 1..=2 {
        let target = app.world().get::<IrohEndpoint>(endpoint_b).unwrap().addr();
        let connect = app
            .world()
            .get::<IrohEndpoint>(endpoint_a)
            .unwrap()
            .connect(target, ALPN);
        let outgoing = app.world_mut().spawn_empty().id();
        connect.apply(app.world_mut().entity_mut(outgoing));
        wait_until(&mut app, |world| {
            world.resource::<Requests>().0.len() == count
                && world
                    .resource::<Requests>()
                    .0
                    .iter()
                    .all(|(entity, _)| world.get::<IrohIo>(*entity).is_some())
        });
    }
    let requests = &app.world().resource::<Requests>().0;
    let [(_, first), (_, second)] = requests.as_slice() else {
        panic!("expected two requests");
    };
    assert_ne!(first, second);
    for &(entity, connection_id) in requests {
        assert_eq!(
            app.world()
                .get::<IrohIo>(entity)
                .unwrap()
                .connection()
                .stable_id(),
            connection_id
        );
    }
}

#[test]
fn connect_send_and_disconnect() {
    const PAYLOAD: &[u8] = b"hello over iroh";
    const DISCONNECT_REASON: &str = "test complete";

    let mut app = test_app(SessionResponse::Accepted);
    let endpoint_a = open_endpoint(&mut app);
    let endpoint_b = open_endpoint(&mut app);
    wait_until(&mut app, |world| {
        world.get::<IrohEndpoint>(endpoint_a).is_some()
            && world.get::<IrohEndpoint>(endpoint_b).is_some()
    });

    let endpoint_a_id = app.world().get::<IrohEndpoint>(endpoint_a).unwrap().id();
    let endpoint_b_id = app.world().get::<IrohEndpoint>(endpoint_b).unwrap().id();
    let target = app.world().get::<IrohEndpoint>(endpoint_b).unwrap().addr();
    let connect = app
        .world()
        .get::<IrohEndpoint>(endpoint_a)
        .unwrap()
        .connect(target, ALPN);
    let outgoing = app.world_mut().spawn_empty().id();
    connect.apply(app.world_mut().entity_mut(outgoing));

    wait_until(&mut app, |world| world.get::<Session>(outgoing).is_some());
    let incoming = find_session(app.world_mut(), SessionSide::Incoming);

    let outgoing_info = app.world().get::<IrohSession>(outgoing).unwrap();
    assert_eq!(outgoing_info.endpoint(), endpoint_a);
    assert_eq!(outgoing_info.peer_id(), endpoint_b_id);
    assert_eq!(outgoing_info.side(), SessionSide::Outgoing);
    let &ChildOf(outgoing_parent) = app.world().get::<ChildOf>(outgoing).unwrap();
    assert_eq!(outgoing_parent, endpoint_a);

    let incoming_info = app.world().get::<IrohSession>(incoming).unwrap();
    assert_eq!(incoming_info.endpoint(), endpoint_b);
    assert_eq!(incoming_info.peer_id(), endpoint_a_id);
    assert_eq!(incoming_info.side(), SessionSide::Incoming);
    let &ChildOf(incoming_parent) = app.world().get::<ChildOf>(incoming).unwrap();
    assert_eq!(incoming_parent, endpoint_b);

    app.world_mut()
        .get_mut::<Session>(outgoing)
        .unwrap()
        .send
        .push(Bytes::from_static(PAYLOAD));
    wait_until(&mut app, |world| {
        world
            .get::<Session>(incoming)
            .is_some_and(|session| session.recv.iter().any(|packet| packet.payload == PAYLOAD))
    });

    app.world_mut()
        .trigger(Disconnect::new(outgoing, DISCONNECT_REASON));
    wait_until(&mut app, |world| world.get_entity(incoming).is_err());

    let disconnects = &app.world().resource::<Disconnects>().0;
    assert!(disconnects.contains(&(
        outgoing,
        RecordedDisconnect::User(DISCONNECT_REASON.to_owned())
    )));
    assert!(disconnects.contains(&(
        incoming,
        RecordedDisconnect::Peer(DISCONNECT_REASON.to_owned())
    )));
}

#[test]
fn reject_incoming_session() {
    const REJECTION_REASON: &str = "not invited";

    let mut app = test_app(SessionResponse::rejected(REJECTION_REASON));
    let endpoint_a = open_endpoint(&mut app);
    let endpoint_b = open_endpoint(&mut app);
    wait_until(&mut app, |world| {
        world.get::<IrohEndpoint>(endpoint_a).is_some()
            && world.get::<IrohEndpoint>(endpoint_b).is_some()
    });

    let target = app.world().get::<IrohEndpoint>(endpoint_b).unwrap().addr();
    let connect = app
        .world()
        .get::<IrohEndpoint>(endpoint_a)
        .unwrap()
        .connect(target, ALPN);
    let outgoing = app.world_mut().spawn_empty().id();
    connect.apply(app.world_mut().entity_mut(outgoing));

    wait_until(&mut app, |world| world.get_entity(outgoing).is_err());
    let disconnects = &app.world().resource::<Disconnects>().0;
    assert!(disconnects.contains(&(
        outgoing,
        RecordedDisconnect::Peer(REJECTION_REASON.to_owned())
    )));
    assert!(
        disconnects.iter().any(|(_, reason)| {
            reason == &RecordedDisconnect::User(REJECTION_REASON.to_owned())
        })
    );
}

#[test]
fn application_streams_are_separate_from_datagrams_and_close_with_the_session() {
    const TOKEN: [u8; 2048] = [42; 2048];
    const ACK: &[u8] = b"token received";
    const DATAGRAM: &[u8] = b"gameplay datagram";

    let mut app = test_app(SessionResponse::Accepted);
    let endpoint_a = open_endpoint(&mut app);
    let endpoint_b = open_endpoint(&mut app);
    wait_until(&mut app, |world| {
        world.get::<IrohEndpoint>(endpoint_a).is_some()
            && world.get::<IrohEndpoint>(endpoint_b).is_some()
    });
    let target = app.world().get::<IrohEndpoint>(endpoint_b).unwrap().addr();
    let connect = app
        .world()
        .get::<IrohEndpoint>(endpoint_a)
        .unwrap()
        .connect(target, ALPN);
    let outgoing = app.world_mut().spawn_empty().id();
    connect.apply(app.world_mut().entity_mut(outgoing));
    assert!(app.world().get::<IrohIo>(outgoing).is_none());
    wait_until(&mut app, |world| world.get::<Session>(outgoing).is_some());
    let incoming = find_session(app.world_mut(), SessionSide::Incoming);
    wait_until(&mut app, |world| world.get::<Session>(incoming).is_some());

    let outgoing_connection = app
        .world()
        .get::<IrohIo>(outgoing)
        .unwrap()
        .connection()
        .clone();
    let incoming_connection = app
        .world()
        .get::<IrohIo>(incoming)
        .unwrap()
        .connection()
        .clone();
    assert_eq!(
        outgoing_connection.remote_id(),
        app.world().get::<IrohSession>(outgoing).unwrap().peer_id()
    );
    assert_eq!(
        incoming_connection.remote_id(),
        app.world().get::<IrohSession>(incoming).unwrap().peer_id()
    );

    let (tx, mut rx) = oneshot::channel();
    app.world().resource::<IrohRuntime>().spawn_on_self({
        let outgoing_connection = outgoing_connection.clone();
        let incoming_connection = incoming_connection.clone();
        async move {
            type StreamError = Box<dyn core::error::Error + Send + Sync>;
            let result = futures::try_join!(
                async {
                    let (mut send, mut recv) = outgoing_connection.open_bi().await?;
                    send.write_all(&TOKEN).await?;
                    send.finish()?;
                    Ok::<_, StreamError>(recv.read_to_end(ACK.len()).await?)
                },
                async {
                    let (mut send, mut recv) = incoming_connection.accept_bi().await?;
                    let token = recv.read_to_end(TOKEN.len()).await?;
                    send.write_all(ACK).await?;
                    send.finish()?;
                    Ok::<_, StreamError>(token)
                },
            );
            _ = tx.send(result.map_err(|error| error.to_string()));
        }
    });
    let mut received = None;
    wait_until(&mut app, |world| {
        for entity in [outgoing, incoming] {
            assert!(world.get::<Session>(entity).unwrap().recv.is_empty());
        }
        received = rx.try_recv().expect("stream task should finish");
        received.is_some()
    });
    let (ack, token) = received.unwrap().unwrap();
    assert_eq!(ack, ACK);
    assert_eq!(token, TOKEN);

    app.world_mut()
        .get_mut::<Session>(outgoing)
        .unwrap()
        .send
        .push(Bytes::from_static(DATAGRAM));
    wait_until(&mut app, |world| {
        world
            .get::<Session>(incoming)
            .unwrap()
            .recv
            .iter()
            .any(|packet| packet.payload == DATAGRAM)
    });

    let (tx, mut rx) = oneshot::channel();
    app.world().resource::<IrohRuntime>().spawn_on_self({
        let connection = incoming_connection.clone();
        async move {
            _ = tx.send(connection.accept_bi().await.is_err());
        }
    });
    app.world_mut()
        .trigger(Disconnect::new(outgoing, "test complete"));
    let mut pending_stream_failed = false;
    wait_until(&mut app, |world| {
        if !pending_stream_failed
            && let Some(failed) = rx.try_recv().expect("stream task should finish")
        {
            assert!(failed, "accepting a stream should fail after disconnect");
            pending_stream_failed = true;
        }
        world.get_entity(outgoing).is_err()
            && world.get_entity(incoming).is_err()
            && pending_stream_failed
    });
    assert!(outgoing_connection.close_reason().is_some());
    assert!(incoming_connection.close_reason().is_some());
}

fn test_app(response: SessionResponse) -> App {
    let mut app = App::new();
    app.add_plugins((MinimalPlugins, IrohPlugin))
        .init_resource::<Disconnects>()
        .init_resource::<Requests>()
        .add_observer(
            move |mut request: On<SessionRequest>, mut requests: ResMut<Requests>| {
                requests
                    .0
                    .push((request.session_entity, request.connection_id));
                request.respond(response.clone());
            },
        )
        .add_observer(
            |trigger: On<Disconnected>, mut disconnects: ResMut<Disconnects>| {
                let reason = match &trigger.reason {
                    DisconnectReason::ByUser(reason) => RecordedDisconnect::User(reason.clone()),
                    DisconnectReason::ByPeer(reason) => RecordedDisconnect::Peer(reason.clone()),
                    DisconnectReason::ByError(error) => {
                        RecordedDisconnect::Error(format!("{error:#}"))
                    }
                };
                disconnects.0.push((trigger.event_target(), reason));
            },
        );
    app
}

fn open_endpoint(app: &mut App) -> Entity {
    let entity = app.world_mut().spawn_empty().id();
    let builder = iroh::Endpoint::builder(presets::Minimal).alpns(vec![ALPN.to_vec()]);
    IrohEndpoint::open(builder).apply(app.world_mut().entity_mut(entity));
    entity
}

fn find_session(world: &mut World, side: SessionSide) -> Entity {
    world
        .query::<(Entity, &IrohSession)>()
        .iter(world)
        .find_map(|(entity, session)| (session.side() == side).then_some(entity))
        .expect("session should exist")
}

fn wait_until(app: &mut App, mut condition: impl FnMut(&mut World) -> bool) {
    let deadline = Instant::now()
        .checked_add(TIMEOUT)
        .expect("test timeout fits in an Instant");
    loop {
        app.update();
        if condition(app.world_mut()) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for condition");
        thread::sleep(Duration::from_millis(5));
    }
}
