import hydra
from omegaconf import DictConfig, OmegaConf
from dataxml.common import Task


class MyApp(Task):

    def launch(self):
        db = self.conf.input.database
        table = self.conf.input.table
        print(db)
        print(table)


@hydra.main(version_base=None, config_path="../../conf/tasks", config_name="mltask_config")
def entrypoint(cfg: DictConfig):
    task = MyApp(init_conf=cfg)
    task.launch()


if __name__ == "__main__":
    entrypoint()
