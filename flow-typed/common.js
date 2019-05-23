declare var IS_DEV_MODE: boolean;
declare var IS_MASTER: boolean;
declare var OPTIONS: Object;
declare var globThrowError: Function;
declare var showError: Function;
declare var showSuccessMessage: Function;
declare var whenSystemReady: (Function) => void;

declare type ClusterMessage = Object & {
  action: string,
};
