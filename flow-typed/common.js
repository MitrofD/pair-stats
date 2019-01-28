declare var DATA_PATH: string;
declare var IS_DEV_MODE: boolean;
declare var IS_MASTER: boolean;
declare var ROOT_PATH: string;
declare var globErrorHandler: Function;

declare type ClusterMessage = {
  action: string,
  data: any,
};
