from data_converge.dag import DAGExecutor
from data_converge.scheduler import TaskScheduler
from .test_state import NOW, graph, open_store


def test_live_scheduler_runs_normal_work_before_each_background_window(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    try:
        scheduler = TaskScheduler(dag, builder, DAGExecutor(dag, lambda i: None, state_store=store),
                                  state_store=store, start_time=NOW)
        assert hasattr(scheduler, "background_worker")
        sequence = []
        scheduler.tick = lambda: sequence.append("normal")

        def background():
            sequence.append("manual")
            scheduler.shutdown()

        scheduler.background_worker = background
        scheduler.start()
        assert sequence == ["normal", "manual"]
    finally:
        store.close()


def test_shutdown_from_normal_work_prevents_new_background_dispatch(tmp_path):
    builder, dag = graph()
    store = open_store(tmp_path / "state.db")
    try:
        scheduler = TaskScheduler(dag, builder, DAGExecutor(dag, lambda i: None, state_store=store),
                                  state_store=store, start_time=NOW)
        assert hasattr(scheduler, "background_worker")
        sequence = []
        scheduler.tick = scheduler.shutdown
        scheduler.background_worker = lambda: sequence.append("manual")
        scheduler.start()
        assert sequence == []
    finally:
        store.close()
