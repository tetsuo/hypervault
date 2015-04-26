if __name__ == '__main__':
  import nose
  import nose.loader
  config = nose.config.Config()
  config.verbosity = 2
  config.stopOnError = True
  nose.run(config=config)