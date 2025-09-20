from src.pipeline.main_pipeline import ETLPipeline


def main():
    pipeline = ETLPipeline("config/config.yaml")
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()
