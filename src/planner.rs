//! The memory planner. It is a pure function: it reads a snapshot and
//! returns a decision. The scheduler does the actions.

/// The state of one engine, as the planner sees it.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Res {
    Unloaded,
    Loading,
    Idle,
    Busy,
    Stopping,
}

#[derive(Clone, Debug)]
pub struct EngineView {
    /// The configured cost or the highest footprint seen, the larger of the two.
    pub cost_mb: u64,
    pub res: Res,
    /// The scheduler permits the eviction: the engine is idle, it is not
    /// persistent, its queue is empty and its minimum resident time is over.
    pub evictable: bool,
    pub evict_cost: u32,
    /// A counter that grows with time. A low value means "used long ago".
    pub last_used: u64,
}

#[derive(Clone, Debug)]
pub struct Mem {
    pub budget_mb: u64,
    pub headroom_mb: u64,
    /// The memory that the system can give now. `None` = not known.
    pub available_mb: Option<u64>,
    pub pressure: u32,
    /// The hub does not load engines at or above this pressure level.
    pub refuse_at: u32,
}

#[derive(PartialEq, Debug)]
pub enum Plan {
    /// Start the engine now.
    Load,
    /// Stop these engines first. Then ask again.
    Evict(Vec<usize>),
    /// Do nothing now. Ask again later.
    Wait(&'static str),
    /// The engine can never load with this config.
    Never(&'static str),
}

/// Decides how to make the engine `target` resident.
pub fn plan(target: usize, engines: &[EngineView], mem: &Mem) -> Plan {
    let cost = engines[target].cost_mb;
    if cost > mem.budget_mb {
        return Plan::Never("the cost of the engine is above the memory budget");
    }
    if mem.pressure >= mem.refuse_at {
        return Plan::Wait("memory pressure is critical");
    }
    let others = || engines.iter().enumerate().filter(|(i, _)| *i != target);
    // An engine that stops still holds its memory. Do one swap at a time.
    if others().any(|(_, e)| e.res == Res::Stopping) {
        return Plan::Wait("an engine is stopping");
    }
    let held: u64 = others().filter(|(_, e)| e.res != Res::Unloaded).map(|(_, e)| e.cost_mb).sum();
    let fits = |freed: u64| {
        held - freed + cost <= mem.budget_mb
            && mem.available_mb.map_or(true, |a| a + freed >= cost + mem.headroom_mb)
    };
    if fits(0) {
        return Plan::Load;
    }
    let mut candidates: Vec<(usize, &EngineView)> =
        others().filter(|(_, e)| e.res == Res::Idle && e.evictable).collect();
    candidates.sort_by_key(|(_, e)| (e.evict_cost, e.last_used));
    let mut freed = 0;
    let mut victims = Vec::new();
    for (i, e) in candidates {
        victims.push(i);
        freed += e.cost_mb;
        if fits(freed) {
            return Plan::Evict(victims);
        }
    }
    if held > freed {
        // Some resident engine is busy, is loading, or cannot be evicted now.
        Plan::Wait("no engine can be evicted now")
    } else {
        Plan::Wait("the system does not have enough available memory")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const QWEN: usize = 0;
    const PARAKEET: usize = 1;
    const ALIGNER: usize = 2;

    fn view(cost_mb: u64, res: Res, evict_cost: u32) -> EngineView {
        EngineView { cost_mb, res, evictable: res == Res::Idle, evict_cost, last_used: 0 }
    }

    /// The three engines of the M1 with the M0 costs.
    fn m1(qwen: Res, parakeet: Res, aligner: Res) -> Vec<EngineView> {
        let mut v = vec![view(2500, qwen, 5), view(300, parakeet, 9), view(3400, aligner, 1)];
        v[PARAKEET].evictable = false; // persistent
        v
    }

    fn mem(budget_mb: u64) -> Mem {
        Mem { budget_mb, headroom_mb: 400, available_mb: Some(8000), pressure: 1, refuse_at: 4 }
    }

    #[test]
    fn an_engine_loads_when_it_fits() {
        let e = m1(Res::Unloaded, Res::Unloaded, Res::Unloaded);
        assert_eq!(plan(QWEN, &e, &mem(5200)), Plan::Load);
        assert_eq!(plan(ALIGNER, &e, &mem(5200)), Plan::Load);
    }

    #[test]
    fn qwen_and_parakeet_fit_together() {
        assert_eq!(plan(QWEN, &m1(Res::Unloaded, Res::Idle, Res::Unloaded), &mem(5200)), Plan::Load);
        assert_eq!(plan(PARAKEET, &m1(Res::Busy, Res::Unloaded, Res::Unloaded), &mem(5200)), Plan::Load);
    }

    #[test]
    fn parakeet_and_the_aligner_fit_together() {
        assert_eq!(plan(ALIGNER, &m1(Res::Unloaded, Res::Idle, Res::Unloaded), &mem(5200)), Plan::Load);
    }

    #[test]
    fn the_aligner_evicts_an_idle_qwen() {
        let e = m1(Res::Idle, Res::Idle, Res::Unloaded);
        assert_eq!(plan(ALIGNER, &e, &mem(5200)), Plan::Evict(vec![QWEN]));
    }

    #[test]
    fn qwen_evicts_an_idle_aligner() {
        let e = m1(Res::Unloaded, Res::Idle, Res::Idle);
        assert_eq!(plan(QWEN, &e, &mem(5200)), Plan::Evict(vec![ALIGNER]));
    }

    #[test]
    fn a_busy_or_loading_engine_is_not_evicted() {
        for res in [Res::Busy, Res::Loading] {
            let e = m1(res, Res::Idle, Res::Unloaded);
            assert!(matches!(plan(ALIGNER, &e, &mem(5200)), Plan::Wait(_)));
        }
    }

    #[test]
    fn an_engine_inside_its_minimum_resident_time_is_not_evicted() {
        let mut e = m1(Res::Idle, Res::Unloaded, Res::Unloaded);
        e[QWEN].evictable = false;
        assert!(matches!(plan(ALIGNER, &e, &mem(5200)), Plan::Wait(_)));
    }

    #[test]
    fn the_planner_waits_for_an_engine_that_stops() {
        let e = m1(Res::Stopping, Res::Idle, Res::Unloaded);
        assert!(matches!(plan(ALIGNER, &e, &mem(5200)), Plan::Wait(_)));
    }

    #[test]
    fn a_persistent_engine_is_never_a_victim() {
        // Budget 2600: Qwen (2500) and Parakeet (300) do not fit together.
        let e = m1(Res::Unloaded, Res::Idle, Res::Unloaded);
        assert!(matches!(plan(QWEN, &e, &mem(2600)), Plan::Wait(_)));
    }

    #[test]
    fn a_low_budget_swaps_qwen_and_parakeet() {
        let mut e = m1(Res::Unloaded, Res::Idle, Res::Unloaded);
        e[PARAKEET].evictable = true; // not persistent in this config
        assert_eq!(plan(QWEN, &e, &mem(2600)), Plan::Evict(vec![PARAKEET]));
        let e = m1(Res::Idle, Res::Unloaded, Res::Unloaded);
        assert_eq!(plan(PARAKEET, &e, &mem(2600)), Plan::Evict(vec![QWEN]));
    }

    #[test]
    fn an_engine_above_the_budget_never_loads() {
        let e = m1(Res::Unloaded, Res::Unloaded, Res::Unloaded);
        assert!(matches!(plan(ALIGNER, &e, &mem(3000)), Plan::Never(_)));
    }

    #[test]
    fn low_available_memory_blocks_or_evicts() {
        let mut m = mem(5200);
        m.available_mb = Some(2000); // Qwen needs 2500 + 400
        let e = m1(Res::Unloaded, Res::Unloaded, Res::Unloaded);
        assert!(matches!(plan(QWEN, &e, &m), Plan::Wait(_)));
        // The idle aligner gives 3400 MB back: 2000 + 3400 >= 2900.
        let e = m1(Res::Unloaded, Res::Unloaded, Res::Idle);
        assert_eq!(plan(QWEN, &e, &m), Plan::Evict(vec![ALIGNER]));
    }

    #[test]
    fn critical_pressure_blocks_a_load() {
        let mut m = mem(5200);
        m.pressure = 4;
        let e = m1(Res::Unloaded, Res::Unloaded, Res::Unloaded);
        assert!(matches!(plan(QWEN, &e, &m), Plan::Wait(_)));
    }

    #[test]
    fn victims_go_by_evict_cost_then_by_age() {
        let mut e = vec![view(1000, Res::Idle, 2), view(1000, Res::Idle, 1), view(1000, Res::Idle, 1), view(1500, Res::Unloaded, 1)];
        e[1].last_used = 9;
        e[2].last_used = 3;
        // Budget 3500: one victim is enough. Engine 2 has the low cost and is the older one.
        assert_eq!(plan(3, &e, &mem(3500)), Plan::Evict(vec![2]));
        // Budget 2500: two victims.
        assert_eq!(plan(3, &e, &mem(2500)), Plan::Evict(vec![2, 1]));
    }

    /// Runs the planner as the scheduler does, on random events, and checks the
    /// budget after each step.
    #[test]
    fn qwen_and_a_3400_mb_engine_are_never_resident_together_under_a_5200_mb_budget() {
        let m = mem(5200);
        let mut e = m1(Res::Unloaded, Res::Unloaded, Res::Unloaded);
        let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut rand = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let mut loads = [0u32; 3];
        for step in 0..20_000u64 {
            let i = rand(3) as usize;
            match rand(4) {
                // A job arrives for engine i.
                0 | 1 => match e[i].res {
                    Res::Unloaded => match plan(i, &e, &m) {
                        Plan::Load => {
                            e[i].res = Res::Loading;
                            loads[i] += 1;
                        }
                        Plan::Evict(victims) => {
                            for v in victims {
                                assert_eq!(e[v].res, Res::Idle, "only an idle engine is a victim");
                                assert_ne!(v, PARAKEET, "a persistent engine is not a victim");
                                e[v].res = Res::Stopping;
                            }
                        }
                        Plan::Wait(_) => {}
                        Plan::Never(why) => panic!("{why}"),
                    },
                    Res::Idle => e[i].res = Res::Busy,
                    _ => {}
                },
                // Engine i makes progress.
                _ => {
                    e[i].res = match e[i].res {
                        Res::Loading | Res::Busy => Res::Idle,
                        Res::Stopping => Res::Unloaded,
                        other => other,
                    };
                    e[i].last_used = step;
                }
            }
            for (i, v) in e.iter_mut().enumerate() {
                v.evictable = v.res == Res::Idle && i != PARAKEET;
            }
            let resident = |i: usize| e[i].res != Res::Unloaded;
            assert!(!(resident(QWEN) && resident(ALIGNER)), "Qwen and the aligner are resident together at step {step}");
            let held: u64 = e.iter().filter(|v| v.res != Res::Unloaded).map(|v| v.cost_mb).sum();
            assert!(held <= m.budget_mb, "{held} MB resident at step {step}");
        }
        // Qwen and the aligner got their turns: the swap works in both directions.
        // Parakeet is persistent, so it loads one time.
        assert!(loads[QWEN] > 10 && loads[ALIGNER] > 10 && loads[PARAKEET] == 1, "loads: {loads:?}");
    }
}
